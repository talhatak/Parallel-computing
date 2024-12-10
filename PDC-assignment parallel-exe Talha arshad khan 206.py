#PARALLEL APPROACH
import pandas as pd
from concurrent.futures import ProcessPoolExecutor

# Load the CSV files
students_df = pd.read_csv('C:\\Users\\Talha\'s Lap\\Desktop\\pdc assignment\\students.csv')
fees_df = pd.read_csv('C:\\Users\\Talha\'s Lap\\Desktop\\pdc assignment\\fees.csv')

# Ensure student_id columns are integers and trim any whitespace
students_df["student_id"] = students_df["student_id"].astype(str).str.strip().astype(int)
fees_df["student_id"] = fees_df["student_id"].astype(str).str.strip().astype(int)

# Debugging: Print unique student IDs for verification
print("Unique Student IDs in students_df:", students_df["student_id"].unique())
print("Unique Student IDs in fees_df:", fees_df["student_id"].unique())

# Preprocess fees data to find the most relevant fee date for each student
def get_most_relevant_date(group):
    date_counts = group["fee_submission_date"].value_counts()
    if all(date_counts == 1):  # If all dates are unique
        return group["fee_submission_date"].max()  # Pick the latest date
    else:
        return date_counts.idxmax()  # Pick the most frequent date

# Create a mapping of student_id to the most relevant fee date
most_relevant_dates = fees_df.groupby("student_id").apply(get_most_relevant_date).reset_index()
most_relevant_dates.columns = ["student_id", "most_relevant_date"]

# Parallelized function to process each student
def process_student(student_row):
    student_id = student_row["student_id"]

    if pd.notna(student_id):  # Ensure the student ID is valid
        # Check if the student ID exists in the precomputed relevant dates
        relevant_date_row = most_relevant_dates[most_relevant_dates["student_id"] == student_id]

        if not relevant_date_row.empty:
            most_relevant_date = relevant_date_row["most_relevant_date"].iloc[0]
            return f"Student ID {student_id}: Most relevant date of payment: {most_relevant_date}"
        else:
            return f"Student ID {student_id}: No fee records found."
    else:
        return f"Invalid Student ID: {student_id}"

# Execute processing in parallel
if __name__ == "__main__":
    # Convert students dataframe to list of dictionaries (rows) for parallel processing
    student_rows = students_df.to_dict("records")

    # Use ProcessPoolExecutor for multiprocessing
    with ProcessPoolExecutor() as executor:
        results = list(executor.map(process_student, student_rows))

    # Print results
    for result in results:
        print(result)
