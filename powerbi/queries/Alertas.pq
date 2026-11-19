let
    Source = Json.Document(Web.Contents(BaseUrl, [RelativePath = "powerbi/alerts", Headers = [#"ngrok-skip-browser-warning" = "1"]])),
    Schema = type table [business_id = text, name = nullable text, category = nullable text, probability = number, threshold = nullable number, event_date = nullable text, created_at = nullable text, lat = number, lon = number],
    Rows = Table.FromRecords(Source, Schema, MissingField.UseNull)
in
    Rows
