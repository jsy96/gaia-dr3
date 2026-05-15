import pandas as pd

def split_by_mag(input_path, mag_limits=(13, 12, 11, 10)):
    df = pd.read_csv(input_path)
    total = len(df)
    print(f"原始恒星数: {total}")

    for mag in mag_limits:
        df_filtered = df[df['phot_g_mean_mag'] <= mag]
        output_path = rf'D:\a_star_catalog\gaia_dr3_{mag}.csv'
        df_filtered.to_csv(output_path, index=False)
        print(f"星等 ≤ {mag}: {len(df_filtered)} 颗 → {output_path}")

if __name__ == '__main__':
    split_by_mag(r'D:\a_star_catalog\gaia_dr3_fill0.csv')
