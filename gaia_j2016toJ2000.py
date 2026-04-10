#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Download Gaia DR3 stars with G<20, keep essential columns,
propagate positions from epoch J2016.0 to J2000.0
with <0.1 mas accuracy, and save results.

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
# 1. Download Gaia stars (G<20) with only required columns
# ----------------------------------------------------------------------
def download_gaia_mag20(output_csv="gaia_mag20_sample.csv",
                        limit=1000,
                        ra_center=100.0,
                        dec_center=0.0,
                        radius_deg=5.0):
    """
    Download a subset of Gaia DR3 stars with phot_g_mean_mag < 20
    in a cone around (ra_center, dec_center).
    """
    if os.path.exists(output_csv):
        print(f"{output_csv} already exists, skip download.")
        return

    Gaia.MAIN_GAIA_TABLE = "gaiadr3.gaia_source"

    query = f"""
    SELECT TOP {limit}
        source_id,
        ra, dec,
        parallax,
        pmra, pmdec,
        radial_velocity,
        phot_g_mean_mag
    FROM gaiadr3.gaia_source
    WHERE phot_g_mean_mag < 20
      AND 1=CONTAINS(
            POINT('ICRS', ra, dec),
            CIRCLE('ICRS', {ra_center}, {dec_center}, {radius_deg})
          )
    """
    print("Querying Gaia archive …")
    job = Gaia.launch_job_async(query)
    results = job.get_results()
    results.to_pandas().to_csv(output_csv, index=False)
    print(f"Saved {len(results)} rows to {output_csv}")

# ----------------------------------------------------------------------
# 2. Propagate to J2000.0
# ----------------------------------------------------------------------
def propagate_to_j2000(input_csv="gaia_mag20_sample.csv",
                       output_csv="gaia_mag20_J2000.csv",
                       chunk_size=50000):
    """
    Read Gaia CSV, propagate coordinates to J2000.0 and save new CSV.
    """
    src_epoch = Time("J2016.0")
    tgt_epoch = Time("J2000.0")

    out_f = open(output_csv, "w", encoding="utf-8")
    out_f.write("source_id,ra_j2000,dec_j2000,dist_pc,phot_g_mean_mag\n")

    for chunk in pd.read_csv(input_csv, chunksize=chunk_size):
        # Filter stars with positive parallax for distance calculation
        good = chunk[chunk["parallax"] > 0].copy()

        sc = SkyCoord(
            ra=good["ra"].values * u.deg,
            dec=good["dec"].values * u.deg,
            pm_ra_cosdec=good["pmra"].values * u.mas/u.yr,
            pm_dec=good["pmdec"].values * u.mas/u.yr,
            distance=(1000.0 / good["parallax"].values) * u.pc,
            radial_velocity=np.where(np.isfinite(good["radial_velocity"]),
                                     good["radial_velocity"], 0.0) * u.km/u.s,
            frame="icrs",
            obstime=src_epoch
        )

        sc_j2000 = sc.apply_space_motion(tgt_epoch)

        for sid, ra, dec, dist, gmag in zip(
            good["source_id"], sc_j2000.ra.deg, sc_j2000.dec.deg,
            sc_j2000.distance.pc, good["phot_g_mean_mag"]
        ):
            out_f.write(f"{sid},{ra:.10f},{dec:.10f},{dist:.6f},{gmag:.3f}\n")

    out_f.close()
    print(f"Propagated positions saved to {output_csv}")

# ----------------------------------------------------------------------
# 3. Main
# ----------------------------------------------------------------------
if __name__ == "__main__":
    # Step 1: download stars (adjust limit/region as needed)
    download_gaia_mag20(
        output_csv="gaia_mag20_sample.csv",
        limit=1000,
        ra_center=100.0,    # center RA in degrees
        dec_center=0.0,     # center Dec in degrees
        radius_deg=5.0      # search radius in degrees
    )

    # Step 2: propagate to J2000.0
    propagate_to_j2000(
        input_csv="gaia_mag20_sample.csv",
        output_csv="gaia_mag20_J2000.csv"
    )

    print("All done.")
