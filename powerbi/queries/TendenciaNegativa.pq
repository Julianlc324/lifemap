let
    Source = Json.Document(Web.Contents(BaseUrl, [RelativePath = "powerbi/negative_trend", Headers = [#"ngrok-skip-browser-warning" = "1"]])),
    Schema = type table [window_start = text, reviews = number, negative = number, pct_negative = nullable number],
    Rows = Table.FromRecords(Source, Schema, MissingField.UseNull)
in
    Rows
