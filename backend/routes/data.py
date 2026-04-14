from fastapi import APIRouter, Query

from state import _state

router = APIRouter(prefix="/data", tags=["data"])


@router.get("")
def get_data(
    start: str | None = Query(None, description="First year-month to return (YYYY-MM)"),
    end: str | None = Query(None, description="Last year-month to return (YYYY-MM)"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict:
    """Return rows from the merged monthly dataset."""
    df = _state["df"].copy()
    if start:
        df = df[df["year_month"] >= start]
    if end:
        df = df[df["year_month"] <= end]
    total = len(df)
    page = df.iloc[offset : offset + limit]
    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "rows": page.where(page.notna(), None).to_dict(orient="records"),
    }


@router.get("/columns")
def get_columns() -> dict:
    """Return the column names and dtypes of the merged dataset."""
    df = _state["df"]
    return {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)}
