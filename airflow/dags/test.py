from vgsi.vgsi_utils import load_city

def download_city(city, output_parquet_file):
    property_df, building_df, assesment_df, appraisal_df, ownership_df = load_city(city, base_url='https://gis.vgsi.com/newhavenct/',pid_min=10, pid_max=100, delay_seconds=0)

    property_df.to_parquet(output_parquet_file + f"_{city}_property.parquet", index=False)
    building_df.to_parquet(output_parquet_file + f"_{city}_building.parquet", index=False)
    assesment_df.to_parquet(output_parquet_file + f"_{city}_assesment.parquet", index=False)
    appraisal_df.to_parquet(output_parquet_file + f"_{city}_appraisal.parquet", index=False)
    ownership_df.to_parquet(output_parquet_file + f"_{city}_ownership.parquet", index=False)

download_city('newhaven', 'test')