import pandas as pd
import requests


class DataPipeline:
    API_URL = "https://api.nasa.gov/planetary/apod"
    API_KEY = "A4D2OH4Lt8SJiUvmCVPNqW0eolDnxSiCqHC8gwpe"
    VIDEO_EXTENSIONS_LIST = ["mov", "mpg", "wmv", "avi", "mp4", "mkv"]

    def get_data_of_the_day(self) -> dict:
        payload = {
            "api_key": self.API_KEY,
        }
        r = requests.get(self.API_URL, params=payload, timeout=10)
        r.raise_for_status()
        data = r.json()

        return data

    def clean_data(self, data: dict):
        df = pd.json_normalize(data)

        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", utc=True)

        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
        df = df.map(lambda x: x.replace("\n", " ") if isinstance(x, str) else x)
        df = df.map(lambda x: x.replace("  ", " ") if isinstance(x, str) else x)

        # Delete "explanation" in "copyright" as it may appear by mistake in 1995
        if "copyright" in df.columns and "Explanation" in df["copyright"]:
            df["copyright"] = df["copyright"].split(" Explanation")[0]

        # Force the right "media_type" depending on media file extension
        video_extension_pattern = "|".join(self.VIDEO_EXTENSIONS_LIST)
        if "hdurl" in df.columns:
            df.loc[
                df["hdurl"].str.contains(video_extension_pattern, regex=True),
                "media_type",
            ] = "video"
        elif "url" in df.columns:
            df.loc[
                df["hd"].str.contains(video_extension_pattern, regex=True), "media_type"
            ] = "video"

        return df


if __name__ == "__main__":
    pipe = DataPipeline()
    data = pipe.get_data_of_the_day()
    df = pipe.clean_data(data)

    print(df["explanation"][0])
    print(f"\n{df.T}")
    print(f"\n{df.dtypes}")
