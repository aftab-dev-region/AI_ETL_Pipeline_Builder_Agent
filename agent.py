
import os
import re
import pandas as pd

from typing import TypedDict, Any
from datetime import datetime, date

from dotenv import load_dotenv

from sqlalchemy import (
    create_engine,
    inspect,
    text,
    Integer,
    Date,
    DateTime,
    Time
)
from sqlalchemy.engine import URL

from langgraph.graph import StateGraph, START, END


load_dotenv()


# =====================================================
# 1. AGENT STATE
# =====================================================

class AgentState(TypedDict, total=False):
    dataframe: Any
    target_table: str
    target_schema: list
    target_columns: list
    evaluation_passed: bool
    evaluation_errors: list
    transformed_rows: list
    row_count: int
    dropped_rows: int
    inserted_rows: int
    load_error: str
    transform_error: str
    final_answer: str


# =====================================================
# 2. MAIN ETL PIPELINE CLASS
# =====================================================

class run_pipeline:

    def __init__(self, target_table: str, batch_size: int = 50):

        self.target_table = target_table
        self.batch_size = batch_size

        # Validate target table name
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", target_table):
            raise ValueError("Invalid MySQL table name")

        self.engine = self._connect_db()

        self.app = self._build_graph()


    # =================================================
    # 3. DATABASE CONNECTION
    # =================================================

    def _connect_db(self):

        required_env = [
            "MYSQL_USER",
            "MYSQL_PASSWORD",
            "MYSQL_HOST",
            "MYSQL_DATABASE"
        ]

        missing = [
            key for key in required_env
            if not os.getenv(key)
        ]

        if missing:
            raise ValueError(
                f"Missing database environment variables: {missing}"
            )

        url = URL.create(
            drivername="mysql+pymysql",
            username=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            host=os.getenv("MYSQL_HOST"),
            port=int(os.getenv("MYSQL_PORT", "3306")),
            database=os.getenv("MYSQL_DATABASE")
        )

        return create_engine(url, pool_pre_ping=True)


    # =================================================
    # 4. GET TARGET TABLE SCHEMA
    # =================================================

    def _get_schema(self, state: AgentState):

        inspector = inspect(self.engine)

        if not inspector.has_table(self.target_table):
            raise ValueError(
                f"Target table '{self.target_table}' does not exist"
            )

        schema = inspector.get_columns(self.target_table)

        return {
            "target_schema": schema
        }


    # =================================================
    # 5. EVALUATE AGENT
    # =================================================

    def _evaluate_agent(self, state: AgentState):

        df = state["dataframe"]

        errors = []

        schema = state["target_schema"]

        # Get MySQL column names
        mysql_columns = {
            col["name"] for col in schema
        }

        csv_columns = set(df.columns)

        # Find missing mandatory MySQL columns
        required_columns = {
            col["name"]
            for col in schema
            if (
                not col["nullable"]
                and col["default"] is None
                and col["name"] not in {
                    "id"
                }  # Auto-generated ID convention
            )
            and col["name"] not in csv_columns
            and col.get("autoincrement") not in (True, "auto")
        }

        missing = required_columns - csv_columns

        if missing:
            errors.append(
                f"Missing required CSV columns: {sorted(missing)}"
            )

        # Check empty CSV
        if df.empty:
            errors.append("CSV file is empty")

        # Check duplicate column names
        if df.columns.duplicated().any():
            errors.append("CSV contains duplicate column names")

        # Check whether CSV has matching target columns
        matching_columns = csv_columns.intersection(mysql_columns)

        if not matching_columns:
            errors.append(
                "No matching columns found between CSV and MySQL table"
            )

        # Validate integer columns
        for col in schema:

            name = col["name"]

            if name not in df.columns:
                continue

            if isinstance(col["type"], Integer):

                values = pd.to_numeric(
                    df[name],
                    errors="coerce"
                )

                invalid = (
                    values.isna() & df[name].notna()
                ) | (
                    values.notna() & (values % 1 != 0)
                )

                if invalid.any():
                    errors.append(
                        f"Column '{name}' contains invalid integer values"
                    )

        return {
            "evaluation_passed": len(errors) == 0,
            "evaluation_errors": errors,
            "target_columns": [
                col["name"]
                for col in schema
                if col["name"] in matching_columns
            ]
        }


    # =================================================
    # 6. TRANSFORM AGENT
    # =================================================

    def _transform_agent(self, state: AgentState):

        if not state["evaluation_passed"]:
            return {}

        df = state["dataframe"].copy()

        dropped_rows = 0

        # Keep only columns available in target table
        columns = state["target_columns"]

        df = df[columns].copy()

        # Remove leading and trailing spaces from text
        for col in df.select_dtypes(include=["object", "string"]).columns:

            df[col] = df[col].apply(
                lambda x: x.strip() if isinstance(x, str) else x
            )

        # Standardize name column
        if "name" in df.columns:

            df["name"] = df["name"].apply(
                lambda x: x.title() if isinstance(x, str) else x
            )

        # Convert date/time columns according to MySQL schema
        for col in state["target_schema"]:

            name = col["name"]

            if name not in df.columns:
                continue

            if isinstance(col["type"], (Date, DateTime, Time)):

                try:
                    df[name] = pd.to_datetime(
                        df[name],
                        errors="raise"
                    )

                    if isinstance(col["type"], Date):
                        df[name] = df[name].dt.date

                    elif isinstance(col["type"], Time):
                        df[name] = df[name].dt.time

                    else:
                        df[name] = df[name].dt.to_pydatetime()

                except Exception as e:

                    return {
                        "transform_error": (
                            f"Date conversion failed for '{name}': {str(e)}"
                        )
                    }

        # Convert Pandas missing values into Python None
        df = df.astype(object).where(
            pd.notna(df),
            None
        )

        # Convert DataFrame into list of dictionaries
        rows = df.to_dict(orient="records")

        return {
            "transformed_rows": rows,
            "row_count": len(rows),
            "dropped_rows": dropped_rows
        }


    # =================================================
    # 7. LOAD AGENT
    # =================================================

    def _load_agent(self, state: AgentState):

        if state.get("transform_error"):
            return {
                "load_error": "Loading skipped due to transformation failure."
            }

        rows = state.get("transformed_rows", [])

        if not rows:
            return {
                "inserted_rows": 0,
                "load_error": "No records available for insertion."
            }

        columns = state["target_columns"]

        # Quote table and column identifiers safely
        preparer = self.engine.dialect.identifier_preparer

        table_name = preparer.quote(self.target_table)

        column_names = ", ".join(
            preparer.quote(col) for col in columns
        )

        placeholders = ", ".join(
            f":{col}" for col in columns
        )

        insert_query = text(
            f"""
            INSERT INTO {table_name}
            ({column_names})
            VALUES ({placeholders})
            """
        )

        inserted_rows = 0

        try:

            # Transaction ensures all batches succeed or roll back
            with self.engine.begin() as connection:

                for start in range(0, len(rows), self.batch_size):

                    batch = rows[
                        start:start + self.batch_size
                    ]

                    connection.execute(
                        insert_query,
                        batch
                    )

                    inserted_rows += len(batch)

            return {
                "inserted_rows": inserted_rows,
                "load_error": ""
            }

        except Exception as e:

            return {
                "inserted_rows": 0,
                "load_error": str(e)
            }


    # =================================================
    # 8. GENERATE FINAL ANSWER
    # =================================================

    def _generate_answer(self, state: AgentState):

        #errors = state.get("evaluation_errors", [])

        if state.get("evaluation_errors", []):

            answer = (
                "CSV evaluation failed.\n"
                + "\n".join(state.get("evaluation_errors", []))
            )

        elif state.get("transform_error"):

            answer = (
                "Data transformation failed: "
                + state["transform_error"]
            )

        elif state.get("load_error"):

            answer = (
                "Data loading failed: "
                + state["load_error"]
            )

        else:

            answer = (
                "ETL processing completed successfully!\n"
                f"Target table: {self.target_table}\n"
                f"Records processed: {state.get('row_count', 0)}\n"
                f"Records inserted: {state.get('inserted_rows', 0)}"
            )

        return {
            "final_answer": answer
        }


    # =================================================
    # 9. CONDITIONAL ROUTING
    # =================================================

    def _route_after_evaluation(self, state: AgentState):

        if state["evaluation_passed"]:
            return "transform_agent"

        return "generate_answer"


    def _route_after_transform(self, state: AgentState):

        if state.get("transform_error"):
            return "generate_answer"

        return "load_agent"


    # =================================================
    # 10. LANGGRAPH ORCHESTRATION
    # =================================================

    def _build_graph(self):

        workflow = StateGraph(AgentState)

        workflow.add_node("get_schema", self._get_schema)
        workflow.add_node("evaluate_agent", self._evaluate_agent)
        workflow.add_node("transform_agent", self._transform_agent)
        workflow.add_node("load_agent", self._load_agent)
        workflow.add_node("generate_answer", self._generate_answer)

        workflow.add_edge(START, "get_schema")
        workflow.add_edge("get_schema", "evaluate_agent")
        workflow.add_conditional_edges("evaluate_agent",
            self._route_after_evaluation,
            {
                "transform_agent": "transform_agent",
                "generate_answer": "generate_answer"
            }
        )
        workflow.add_conditional_edges("transform_agent",
            self._route_after_transform,
            {
                "load_agent": "load_agent",
                "generate_answer": "generate_answer"
            }
        )
        workflow.add_edge("load_agent", "generate_answer")
        workflow.add_edge("generate_answer", END)

        return workflow.compile()


    # =================================================
    # 11. PUBLIC METHOD - CSV INPUT
    # =================================================

    def process_csv(self, csv_input):

        # Accept DataFrame, file path or uploaded file
        if isinstance(csv_input, pd.DataFrame):

            df = csv_input.copy()

        else:

            df = pd.read_csv(csv_input)

        # Remove accidental spaces from column names
        df.columns = df.columns.str.strip()

        initial_state = {
            "dataframe": df,
            "target_table": self.target_table
        }

        result = self.app.invoke(initial_state)

        return {
            "answer": result.get("final_answer", ""),
            "evaluation_passed": result.get("evaluation_passed", False),
            "evaluation_errors": result.get("evaluation_errors", []),
            "transform_error": result.get("transform_error", ""),
            "load_error": result.get("load_error", ""),
            "processed_rows": result.get("row_count", 0),
            "inserted_rows": result.get("inserted_rows", 0)
        }
