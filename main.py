from arcgis.features import FeatureLayer
from arcgis.gis import GIS
import pandas as pd


def get_all_features(service, batch_size=200, max_rows=None):
    """Get features from a service in batches.

    Args:
        service: The FeatureLayer service to query
        batch_size: Number of records to fetch per batch
        max_rows: Maximum total number of rows to fetch (None for all rows)
    """
    all_features = []
    offset = 0

    while True:
        # Calculate how many records to fetch in this batch
        if max_rows is not None:
            remaining = max_rows - len(all_features)
            if remaining <= 0:
                break
            current_batch_size = min(batch_size, remaining)
        else:
            current_batch_size = batch_size

        result = service.query(
            out_fields="iden, rotacio, objectid",
            result_record_count=current_batch_size,
            result_offset=offset,
        )

        if not result.features:
            break

        all_features.extend(result.features)
        offset += current_batch_size

        if len(result.features) < current_batch_size:
            break

    return pd.DataFrame([f.as_dict["attributes"] for f in all_features])


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

        # Configure these parameters as needed
        BATCH_SIZE = 200
        MAX_ROWS = 400  # Set to a number to limit total rows, or None for all rows

        print("Fetching features from pre environment...")
        df_pre = get_all_features(service_pre, batch_size=BATCH_SIZE, max_rows=MAX_ROWS)
        print(f"Found {len(df_pre)} features in pre environment")

        print("Fetching features from dev environment...")
        df_dev = get_all_features(service_dev, batch_size=BATCH_SIZE, max_rows=MAX_ROWS)
        print(f"Found {len(df_dev)} features in dev environment")

        # Merge the dataframes on 'iden' to compare 'rotacio' values
        merged_df = pd.merge(df_pre, df_dev, on="iden", suffixes=("_pre", "_dev"))

        # Find records where rotacio values differ
        different_rotacio = merged_df[
            merged_df["rotacio_pre"] != merged_df["rotacio_dev"]
        ]

        print(
            f"\nFound {len(different_rotacio)} records with different rotacio values:"
        )
        for _, row in different_rotacio.iterrows():
            print(f"iden: {row['iden']}")
            print(f"  Pre rotacio: {row['rotacio_pre']}")
            print(f"  Dev rotacio: {row['rotacio_dev']}")
            print(f"  Dev objectid: {row['objectid_dev']}")
            print()

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
