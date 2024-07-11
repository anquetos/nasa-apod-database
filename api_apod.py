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
        self.data = r.json()

        return self.data

    def clean_data(self) -> pd.DataFrame:
        self.df = pd.json_normalize(self.data)

        self.df["date"] = pd.to_datetime(self.df["date"], format="%Y-%m-%d", utc=True)

        self.df = self.df.map(lambda x: x.strip() if isinstance(x, str) else x)
        self.df = self.df.map(
            lambda x: x.replace("\n", " ") if isinstance(x, str) else x
        )
        self.df = self.df.map(
            lambda x: x.replace("  ", " ") if isinstance(x, str) else x
        )

        # Delete "explanation" in "copyright" as it may appear by mistake in 1995
        if "copyright" in self.df.columns and "Explanation" in self.df["copyright"]:
            self.df["copyright"] = self.df["copyright"].split(" Explanation")[0]

        # Force the right "media_type" depending on media file extension
        video_extension_pattern = "|".join(self.VIDEO_EXTENSIONS_LIST)
        if "hdurl" in self.df.columns:
            self.df.loc[
                self.df["hdurl"].str.contains(video_extension_pattern, regex=True),
                "media_type",
            ] = "video"
        elif "url" in self.df.columns:
            self.df.loc[
                self.df["hd"].str.contains(video_extension_pattern, regex=True),
                "media_type",
            ] = "video"

        return self.df

    def get_image_metadata(self):
        if self.df["media_type"][0] == "image":
            print("qsdfqsf")

        return self.df


if __name__ == "__main__":
    pipe = DataPipeline()
    pipe.get_data_of_the_day()
    pipe.clean_data()
    df = pipe.get_image_metadata()

    print(df["explanation"][0])
    print(f"\n{df.T}")
    print(f"\n{df.dtypes}")
    print()
