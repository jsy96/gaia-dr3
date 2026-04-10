#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Download a Gaia DR3 subset, read & process it locally, propagate coordinates
from epoch J2016.0 to J2000.0 with <0.1 mas accuracy, and write to file.

Author: Your Name
"""

import os
import numpy as np
import pandas as pd
from astroquery.gaia import Gaia
from astropy.time import Time
from astropy import units as u
from astropy.coordinates import SkyCoord

# ----------------------------------------------------------------------
# 1. Download a small Gaia DR3 sample via ADQL
# ----------------------------------------------------------------------
def download_gaia_sample(output_csv="gaia_sample.csv", limit=1000):
    """
    Download a subset of Gaia DR3 with RA/Dec around a region, e.g. a 5 deg
    cone around RA=100 deg, Dec=0 deg, just for demo.
    """
    if os.path.exists(output_csv):
        print(f"{output_csv} already exists, skip download.")
        return

    Gaia.MAIN_GAIA_TABLE = "gaiadr3.gaia_source"
    # Example ADQL: small cone for demo
    query = f"""
    SELECT TOP {limit}
        source_id, ra, dec, parallax, pmra, pmdec,
        radial_velocity, ra_error, dec_error
    FROM gaiadr3.gaia_source
    WHERE 1=CONTAINS(
        POINT('ICRS', ra, dec),
        CIRCLE('ICRS', 100, 0, 5))
    """
    print("Querying Gaia archive …")
    job = Gaia.launch_job_async(query)
    r = job.get_results()
    r.to_pandas().to_csv(output_csv, index=False)
    print(f"Saved {len(r)} rows to {output_csv}")

# ----------------------------------------------------------------------
# 2. Read local CSV and propagate to J2000.0
# ----------------------------------------------------------------------
def propagate_to_j2000(input_csv="gaia_sample.csv",
                       output_csv="gaia_sample_J2000.csv",
                       chunk_size=50000):
    """
    Read Gaia CSV, propagate to J2000.0 and save to new CSV.
    chunk_size is for very large tables; we stream chunks to limit memory.
    """
    tgt_epoch = Time("J2000.0")
    src_epoch = Time("J2016.0")

    # open writer
    out_f = open(output_csv, "w", encoding="utf-8")
    out_f.write("source_id,ra_j2000,dec_j2000,dist_pc\n")

    for chunk in pd.read_csv(input_csv, chunksize=chunk_size):
        # Filter out problematic rows
        mask = chunk["parallax"] > 0
        good = chunk[mask].copy()

        # Convert to astropy quantities
        sc = SkyCoord(
            ra=good["ra"].values * u.deg,
            dec=good["dec"].values * u.deg,
            pm_ra_cosdec=good["pmra"].values * u.mas/u.yr,
            pm_dec=good["pmdec"].values * u.mas/u.yr,
            distance=(1000.0/good["parallax"].values) * u.pc,
            radial_velocity=np.where(np.isfinite(good["radial_velocity"]),
                                     good["radial_velocity"], 0.0) * u.km/u.s,
            frame="icrs",
            obstime=src_epoch
        )

        sc_j2000 = sc.apply_space_motion(tgt_epoch)

        for sid, ra, dec, dist in zip(good["source_id"],
                                      sc_j2000.ra.deg,
                                      sc_j2000.dec.deg,
                                      sc_j2000.distance.pc):
            out_f.write(f"{sid},{ra:.10f},{dec:.10f},{dist:.6f}\n")

    out_f.close()
    print(f"Propagated positions saved to {output_csv}")

# ----------------------------------------------------------------------
# 3. Main
# ----------------------------------------------------------------------
if __name__ == "__main__":
    download_gaia_sample("gaia_sample.csv", limit=1000)
    propagate_to_j2000("gaia_sample.csv", "gaia_sample_J2000.csv")
    print("Done.")
