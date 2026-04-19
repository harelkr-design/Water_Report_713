import pandas as pd
import re
import string
from pathlib import Path
import os

def extract_consumer_info(text):
    text = str(text).strip()
    # מחפש תבנית של "צרכן [מספר] - [שם]"
    match = re.search(r'צרכן\s*(\d+)\s*-\s*(.*)', text)
    if match:
        return match.group(1), match.group(2).strip()
    return None, None


def process_single_file(file_path):
    #עיבוד קובץ אקסל בודד של דוח 713
    # חילוץ השנה משם הקובץ
    year_match = re.search(r'(\d{4})', file_path.name)
    current_year = year_match.group(1) if year_match else "Unknown"

    print(f"--- מעבד: שנת {current_year} ({file_path.name}) ---")

    # טעינה ראשונית ללא כותרות
    df = pd.read_excel(file_path, header=None)

    # חילוץ וזיהוי צרכנים (מילוי מטה של פרטי הצרכן לכל שורות הנתונים שלו)
    temp_cols = df[0].apply(lambda x: pd.Series(extract_consumer_info(x)))
    df['Household_ID'] = temp_cols[0].ffill()
    df['Owner_Name'] = temp_cols[1].ffill()
    df['Household_ID'] = pd.to_numeric(df['Household_ID'], errors='coerce')

    # מתן שמות זמניים לעמודות (A, B, C...) כדי לעבוד עם הפורמט הקבוע של הדוח
    num_of_cols = len(df.columns)
    df.columns = list(string.ascii_uppercase)[:num_of_cols - 2] + ['Household_ID', 'Owner_Name']

    # סינון שורות הנתונים (תעריפי מים 1, 2, 4)
    data_pattern = r'^([124][אבג])'
    df_final = df[df['A'].astype(str).str.contains(data_pattern, na=False)].copy()
    df_final['Year'] = current_year

    # לוגיקה לזיהוי "לאחר הקצאה" (כאשר מופיע אותו תעריף פעמיים לאותו צרכן)
    df_final['is_allocation'] = df_final.groupby(['Household_ID', 'A']).cumcount() > 0
    df_final['Base_Type'] = df_final['A'].astype(str).str[0]
    df_final['Rate_type'] = df_final.apply(lambda r: f"{r['A']} לאחר הקצאה" if r['is_allocation'] else r['A'], axis=1)

    # שיוך לקבוצות סיכום לפי לוגיקת ספק המים
    def assign_summary_group(row):
        if row['is_allocation'] and row['Base_Type'] == '2':
            return '4'
        if row['is_allocation'] and row['Base_Type'] == '1':
            return None
        return row['Base_Type']

    df_final['Summary_Group'] = df_final.apply(assign_summary_group, axis=1)

    # המרת חודשי הצריכה למספרים
    month_cols = ['B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M']
    df_final[month_cols] = df_final[month_cols].fillna(0).apply(pd.to_numeric, errors='coerce').fillna(0)
    df_final['year_sum'] = df_final[month_cols].sum(axis=1)

    # יצירת שורות סיכום (Totals)
    summary_data = df_final.dropna(subset=['Summary_Group'])
    summary_rows = summary_data.groupby(['Household_ID', 'Owner_Name', 'Year', 'Summary_Group'])[
        month_cols + ['year_sum']].sum().reset_index()

    summary_rows['Rate_type'] = "סה''כ סוג " + summary_rows['Summary_Group']
    summary_rows['is_summary'] = 1
    df_final['is_summary'] = 0

    # עמודות עזר למיון
    df_final['Sort_Group'] = df_final['Base_Type']
    summary_rows['Sort_Group'] = summary_rows['Summary_Group']

    # איחוד הנתונים ושורות הסיכום
    df_output = pd.concat([df_final, summary_rows], ignore_index=True)

    # תרגום עמודות לחודשים
    english_months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                      'November', 'December']
    month_rename_dict = dict(zip(month_cols, english_months))
    df_output = df_output.rename(columns=month_rename_dict)

    return df_output

def get_user_paths():
    """
    מזהה אוטומטית את נתיבי המערכת של המשתמש הנוכחי
    """
    home = Path.home()
    downloads = home / "Downloads"
    desktop = home / "Desktop"
    return downloads, desktop

def main():
    downloads_path, desktop_path = get_user_paths()

    folder_name = input(f"נא להזין את שם התיקייה שנמצאת בתוך 'הורדות' (למשל 'Water_Files'): ")

    source_folder = downloads_path / folder_name
    output_path = desktop_path / "Water_Report_713.xlsx"

    if not source_folder.exists():
        print(f"שגיאה: התיקייה {source_folder} לא נמצאה.")
        return

    all_dfs = []
    for file in source_folder.glob("*.xlsx"):
        try:
            cleaned_df = process_single_file(file)
            all_dfs.append(cleaned_df)
        except Exception as e:
            print(f"שגיאה בעיבוד {file.name}: {e}")

    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)

        # מיון לוגי: שנה -> משק -> סוג תעריף
        final_df = final_df.sort_values(by=['Year', 'Household_ID', 'Sort_Group', 'is_summary', 'Rate_type'])

        # בחירת עמודות סופית
        english_months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                          'October', 'November', 'December']
        final_cols = ['Year', 'Household_ID', 'Owner_Name', 'Rate_type'] + english_months + ['year_sum']
        final_df = final_df[final_cols]

        # ייצוא
        final_df.to_excel(output_path, index=False)
        print("\n" + "=" * 30)
        print(f"הסתיים בהצלחה!")
        print(f"הקובץ המאוחד נשמר בשולחן העבודה: {output_path.name}")
        print("=" * 30)
    else:
        print("לא נמצאו קבצי אקסל לעיבוד בתיקייה המצוינת.")


if __name__ == "__main__":
    main()