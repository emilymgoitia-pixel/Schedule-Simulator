import pandas as pd

RELATIONSHIPS = [
    {"pred": "Design", "succ": "Permitting", "type": "FS"},
    {"pred": "Permitting", "succ": "Civil", "type": "FS"},
    {"pred": "Civil", "succ": "Shell", "type": "FS"},
    {"pred": "Civil", "succ": "Equipment Yard", "type": "FS"},
    {"pred": "Shell", "succ": "MEP Fitup", "type": "FS"},
    {"pred": "OFCI Procurement", "succ": "MEP Fitup", "type": "FF"},
    {"pred": "MEP Fitup", "succ": "Commissioning", "type": "FS"},
    {"pred": "Site Power", "succ": "Commissioning", "type": "FS"},
    {"pred": "Commissioning", "succ": "Tenant Fitout", "type": "FS"},
]

def recalc_schedule(df):

    df = df.copy()
    df["Start"] = pd.to_datetime(df["Start"])
    df["Finish"] = pd.to_datetime(df["Finish"])

    for i in range(len(df)):
        df["Finish"] = df["Start"] + pd.to_timedelta(df["DurationDays"], unit="D")

    phase_index = {p: i for i, p in enumerate(df["Phase"])}

    for _ in range(10):

        for rel in RELATIONSHIPS:

            pred = rel["pred"]
            succ = rel["succ"]
            rtype = rel["type"]

            if pred not in phase_index or succ not in phase_index:
                continue

            p = phase_index[pred]
            s = phase_index[succ]

            pred_finish = df.loc[p, "Finish"]
            succ_start = df.loc[s, "Start"]
            succ_finish = df.loc[s, "Finish"]
            duration = df.loc[s, "DurationDays"]

            if rtype == "FS":

                required = pred_finish

                if succ_start < required:
                    df.loc[s, "Start"] = required
                    df.loc[s, "Finish"] = required + pd.Timedelta(days=duration)

            if rtype == "FF":

                required = pred_finish

                if succ_finish < required:
                    df.loc[s, "Finish"] = required
                    df.loc[s, "Start"] = required - pd.Timedelta(days=duration)

    return df