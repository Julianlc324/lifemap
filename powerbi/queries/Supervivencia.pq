let
    Source = Json.Document(Web.Contents(BaseUrl, [RelativePath = "powerbi/survival", Headers = [#"ngrok-skip-browser-warning" = "1"]])),
    Schema = type table [group_type = text, group = text, t_days = number, t_years = number, survival = number, ci_low = number, ci_high = number, at_risk = number],
    Rows = Table.FromRecords(Source, Schema, MissingField.UseNull)
in
    Rows
