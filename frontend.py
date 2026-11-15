
import streamlit as st
import agent as demo

st.title("AI ETL Pipeline Builder")

# 1. Get CSV file from user
uploaded_file = st.file_uploader(
    "Upload CSV file",
    type=["csv"]
)

# 2. Get destination table name from user
target_table = st.text_input(
    "Enter MySQL Target Table Name",
    placeholder="Example: employees"
)

batch_size = 50

# 3. Set batch size
# batch_size = st.number_input(
    # "Batch Size",
    # min_value=1,
    # max_value=1000,
    # value=50
# )

# 4. Run ETL Pipeline
if st.button("Run ETL Pipeline"):

    if uploaded_file is None:
        st.warning("Please upload a CSV file.")
        
    elif not target_table.strip():
        st.warning("Please enter a target table name.")

    else:
        pipeline = demo.run_pipeline(
            target_table=target_table.strip(),
            batch_size=batch_size
        )

        result = pipeline.process_csv(uploaded_file)

        st.write(result["answer"])

        if result["evaluation_errors"]:
            st.error(result["evaluation_errors"])

        elif result["load_error"]:
            st.error(result["load_error"])

        else:
            st.success("ETL completed successfully!")
            st.write("Inserted rows:", result["inserted_rows"])
