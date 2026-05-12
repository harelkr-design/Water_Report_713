import pandas as pd
import re
import string
from pathlib import Path
import os

def extract_consumer_info(text):
    text = str(text).strip()
    match = re.search(r'צרכן\s*(\d+)\s*-\s*(.*)', text)
    if match:
        return match.group(1), match.group(2).strip()
    return None, None


def process_single_file(file_path):
    year_match = re.search(r'(\d{4})', file_path.name)
    current_year = year_match.group(1) if year_match else "Unknown"
    print(f"--- מעבד: שנת {current_year} ({file_path.name}) ---")

    df = pd.read_excel(file_path, header=None)

    # חילוץ פרטי צרכן
    temp_cols = df[0].apply(lambda x: pd.Series(extract_consumer_info(x)))
    df['Household_ID'] = temp_cols[0].ffill()
    df['Owner_Name'] = temp_cols[1].ffill()
    df['Household_ID'] = pd.to_numeric(df['Household_ID'], errors='coerce')

    # מתן שמות זמניים לעמודות הקיימות בלבד
    # עמודה 0 היא A, עמודה 1 היא B וכן הלאה
    actual_num_cols = df.shape[1] - 2  # פחות שתי העמודות שהוספנו עכשיו
    temp_column_names = list(string.ascii_uppercase)[:actual_num_cols]
    df.columns = temp_column_names + ['Household_ID', 'Owner_Name']

    # סינון שורות הנתונים - תיקון ה-Regex למניעת אזהרה
    data_pattern = r'^[124][אבג]'
    df_final = df[df['A'].astype(str).str.contains(data_pattern, na=False, regex=True)].copy()
    df_final['Year'] = current_year

    # זיהוי עמודות החודשים הקיימות בפועל (כל מה שבין A ל-Household_ID)
    # בקוד המקורי שלך חודשים התחילו מ-B
    month_cols = [c for c in temp_column_names if c != 'A']

    # המרת נתונים למספרים
    df_final[month_cols] = df_final[month_cols].fillna(0).apply(pd.to_numeric, errors='coerce').fillna(0)
    df_final['year_sum'] = df_final[month_cols].sum(axis=1)

    # תרגום עמודות לחודשים באופן דינמי
    english_months_full = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                           'October', 'November', 'December']
    # לוקחים רק את מספר החודשים שיש בדוח הנוכחי
    current_report_months = english_months_full[:len(month_cols)]
    month_rename_dict = dict(zip(month_cols, current_report_months))

    df_output = df_output.rename(columns=month_rename_dict)

    # מחזירים גם את רשימת השמות של החודשים שהיו בדוח הזה
    return df_output, current_report_months


def main():
    home = Path.home()
    desktop = home / "Desktop"
    source_folder = home / "Downloads" / input("שם התיקייה בתוך הורדות: ")
    output_path = desktop / "Water_Report_713.xlsx"

    all_dfs = []
    all_month_names = set()

    for file in source_folder.glob("*.xlsx"):
        try:
            cleaned_df, months_in_file = process_single_file(file)
            all_dfs.append(cleaned_df)
            all_month_names.update(months_in_file)
        except Exception as e:
            print(f"שגיאה בעיבוד {file.name}: {e}")

    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)

        # סידור רשימת החודשים לפי הסדר הכרונולוגי (למקרה ששולבו דוחות של 4 ו-12 חודשים)
        english_months_order = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                                'October', 'November', 'December']
        existing_months = [m for m in english_months_order if m in final_df.columns]

        final_df = final_df.sort_values(by=['Year', 'Household_ID', 'Sort_Group', 'is_summary', 'Rate_type'])

        final_cols = ['Year', 'Household_ID', 'Owner_Name', 'Rate_type'] + existing_months + ['year_sum']
        final_df = final_df[final_cols]

        final_df.to_excel(output_path, index=False)
        print(f"\nבוצע בהצלחה! הקובץ נשמר ב: {output_path}")


if __name__ == "__main__":
    main()
