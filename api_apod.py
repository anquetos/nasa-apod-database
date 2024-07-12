from io import BytesIO

import pandas as pd
import requests
from PIL import ExifTags, Image

Image.MAX_IMAGE_PIXELS = 933120000


class DataPipeline:
    API_URL = "https://api.nasa.gov/planetary/apod"
    API_KEY = "A4D2OH4Lt8SJiUvmCVPNqW0eolDnxSiCqHC8gwpe"
    VIDEO_EXTENSIONS_LIST = ["mov", "mpg", "wmv", "avi", "mp4", "mkv"]

    def get_data_of_the_day(self) -> pd.DataFrame:
        payload = {
            "api_key": self.API_KEY,
        }
        r = requests.get(self.API_URL, params=payload, timeout=10)
        r.raise_for_status()
        data = r.json()
        self.df = pd.json_normalize(data)

        return self.df

    def get_image_metadata(self):
        # Check if the media is an image
        if self.df["media_type"].values != "image":
            print("The media is not an image.")
            return self.df

        # Check if a valid image url is available
        if pd.notna(self.df["hdurl"].values):
            image_url = self.df["hdurl"].values[0]
        elif pd.notna(self.df["url"].values):
            image_url = self.df["url"].values[0]
        else:
            print("No valid image url available.")
            return self.df

        # Request the image url
        r = requests.get(image_url)

        if r.status_code != 404:
            img = Image.open(BytesIO(r.content))

            # Extract basic information about the image
            self.df["img_width_px"] = img.size[0]
            self.df["img_height_px"] = img.size[1]
            self.df["img_mode"] = img.mode
            self.df["img_format"] = img.format

            # Check if image contains EXIF tags and extract them
            if hasattr(img, "_getexif") and img._getexif():
                exif_data = img._getexif()
                tags_to_extract = ["Make", "Model", "Software"]

                # Look for tags in EXIF data
                extracted_tags = {
                    ExifTags.TAGS.get(tag_id, tag_id): value
                    for tag_id, value in exif_data.items()
                    if ExifTags.TAGS.get(tag_id) in tags_to_extract
                }

                for tag_name, value in extracted_tags.items():
                    self.df[tag_name] = value

        # Rename columns
        self.df = self.df.rename(
            columns={
                "Make": "camera_make",
                "Model": "camera_model",
                "Software": "software",
            },
            errors="ignore",
        )

        return self.df

    def clean_data(self) -> pd.DataFrame:
        self.df["date"] = pd.to_datetime(self.df["date"], format="%Y-%m-%d", utc=True)

        self.df = self.df.map(lambda x: x.strip() if isinstance(x, str) else x)
        self.df = self.df.map(
            lambda x: x.replace("\n", " ") if isinstance(x, str) else x
        )
        self.df = self.df.map(
            lambda x: x.replace("  ", " ") if isinstance(x, str) else x
        )
        self.df = self.df.map(
            lambda x: x.replace("\x00", "") if isinstance(x, str) else x
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


if __name__ == "__main__":
    pipe = DataPipeline()
    pipe.get_data_of_the_day()
    pipe.get_image_metadata()
    df = pipe.clean_data()

    print(df["explanation"][0])
    print(f"\n{df.T}")
