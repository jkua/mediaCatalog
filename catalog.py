import exiftool

files = ["a.jpg", "b.png", "c.tif"]
with exiftool.ExifTool() as et:
    metadata = et.get_metadata_batch(files)