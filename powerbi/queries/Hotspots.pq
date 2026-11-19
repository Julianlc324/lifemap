let
    Source = Json.Document(Web.Contents(BaseUrl, [RelativePath = "powerbi/hotspots", Headers = [#"ngrok-skip-browser-warning" = "1"]])),
    Schema = type table [geohash = text, window_start = text, window_end = text, count = number, negative = number, pct_negative = number, delta_pp = nullable number, hotspot = logical, lat = number, lon = number],
    Rows = Table.FromRecords(Source, Schema, MissingField.UseNull)
in
    Rows
