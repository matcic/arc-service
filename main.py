from arcgis.features import FeatureLayer
from arcgis.gis import GIS
import pandas as pd
from datetime import datetime


def get_features(service):
    result = service.query(
        out_fields="iden, rotacio, objectid",
    )
    return pd.DataFrame([f.as_dict["attributes"] for f in result.features])


def main():
    try:
        print("Connecting to GIS...")
        gis_pre = GIS(
            url="https://sigabpre.aiguesdebarcelona.cat/portal/",
            username="xxxxx",
            password="xxxxx",
        )
        print("Connected to GIS pre")
        gis_dev = GIS(
            url="https://sigabdev.aiguesdebarcelona.cat/portal/",
            username="xxxxx",
            password="xxxxx",
        )
        print("Connected to GIS dev")

        service_pre = FeatureLayer(
            "https://sigabpre.aiguesdebarcelona.cat/server/rest/services/Edicio/AbastamentEdicio/FeatureServer/0",
            gis_pre,
        )
        service_dev = FeatureLayer(
            "https://sigabdev.aiguesdebarcelona.cat/server/rest/services/Edicio/AbastamentEdicio/FeatureServer/0",
            gis_dev,
        )

        print("Fetching features from pre environment...")
        df_pre = get_features(service_pre)
        print(f"Found {len(df_pre)} features in pre environment")

        print("Fetching features from dev environment...")
        df_dev = get_features(service_dev)
        print(f"Found {len(df_dev)} features in dev environment")

        # Merge the dataframes on 'iden' to compare 'rotacio' values
        merged_df = pd.merge(df_pre, df_dev, on="iden", suffixes=("_pre", "_dev"))

        # Find records where rotacio values differ
        different_rotacio = merged_df[
            merged_df["rotacio_pre"] != merged_df["rotacio_dev"]
        ]

        # Select and rename columns for the output
        output_df = different_rotacio[
            ["iden", "rotacio_pre", "rotacio_dev", "objectid_dev"]
        ]

        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"rotacio_differences_{timestamp}.xlsx"

        # Save to Excel
        output_df.to_excel(filename, index=False)
        print(f"\nFound {len(different_rotacio)} records with different rotacio values")
        print(f"Results saved to: {filename}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
