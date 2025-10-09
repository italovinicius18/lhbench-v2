"""
TPC-DS Query Processor
Loads TPC-DS queries from DuckDB repository (already processed, no substitution needed)
"""

import re
from pathlib import Path
from typing import Dict, Any, Tuple, List


class TPCDSQueryProcessor:
    """Process TPC-DS queries - loads from DuckDB pre-processed queries."""

    def __init__(self, scale_factor: int = 1):
        self.scale_factor = scale_factor
        self.queries_dir = Path("/opt/spark/configs/queries/tpcds")

    def _get_default_substitutions(self) -> Dict[str, str]:
        """Get default parameter substitutions based on scale factor."""

        # Base substitutions (scale-independent)
        subs = {
            # Limits
            "_LIMITA": "100",
            "_LIMITB": "100",
            "_LIMITC": "100",
            "_LIMIT": "100",

            # Years
            "YEAR": "2000",
            "YEAR.1": "2000",
            "YEAR.2": "2001",

            # Months
            "MONTH": "1",
            "MONTH.1": "1",
            "MONTH.2": "2",

            # Quarters
            "QTR": "1",

            # Days
            "DY": "1",

            # Numbers
            "NUMBER": "10",

            # State
            "STATE": "TN",
            "STATE.1": "TN",
            "STATE.2": "TN",
            "STATE.3": "TN",

            # County
            "COUNTY": "Williamson County",
            "COUNTY_A": "Williamson County",
            "COUNTY_B": "Franklin County",
            "COUNTY_C": "Bronx County",
            "COUNTY_D": "Orange County",
            "COUNTY_E": "Williamson County",
            "COUNTY_F": "Franklin County",
            "COUNTY_G": "Bronx County",
            "COUNTY_H": "Orange County",

            # Manufacturer
            "MANUFACT": "128",
            "MANUFACTURER": "128",
            "MANUFACTURER.1": "128",
            "MANUFACTURER.2": "129",
            "MANUFACTURER.3": "130",

            # Category
            "CATEGORY": "Sports",
            "CATEGORY.1": "Sports",
            "CATEGORY.2": "Books",
            "CATEGORY.3": "Home",

            # Gender
            "GEN": "M",
            "GENDER": "M",
            "GENDER.1": "M",
            "GENDER.2": "F",

            # Market segment
            "MARKET_SEGMENT": "AUTOMOBILE",

            # GMT offset
            "GMT": "-5",
            "GMT_OFFSET": "-5",

            # DMS (Demographics)
            "DMS": "1200",

            # Color
            "COLOR": "white",
            "COLOR.1": "white",
            "COLOR.2": "black",

            # Education
            "EDUCATION": "Advanced Degree",
            "ES": "4 yr Degree",

            # Marital status
            "MS": "M",
            "MS.1": "M",
            "MS.2": "S",

            # Buy potential
            "BUYPOTENTIAL": ">10000",
            "BPONE": ">10000",
            "BPTWO": "unknown",

            # Dependency count
            "DEPCNT": "5",

            # Vehicle count
            "VEHCNT": "3",

            # Price range
            "PRICE": "100",

            # Aggregation column (Q3)
            "AGGC": "ss_ext_sales_price",

            # Manager ID (Q19)
            "MANAGER": "7",
            "PRICE.1": "100",
            "PRICE.2": "200",
        }

        # Scale-dependent substitutions
        if self.scale_factor >= 1:
            subs.update({
                "SALES": "10000",
                "REVENUE": "100000",
            })

        if self.scale_factor >= 100:
            subs.update({
                "SALES": "100000",
                "REVENUE": "1000000",
            })

        return subs

    def process_query(self, query_text: str, custom_subs: Dict[str, str] = None) -> str:
        """
        Process query by substituting parameters.

        Args:
            query_text: Original query text with placeholders
            custom_subs: Optional custom substitutions to override defaults

        Returns:
            Processed query with substitutions applied
        """
        # Merge custom substitutions if provided
        subs = self.substitutions.copy()
        if custom_subs:
            subs.update(custom_subs)

        # Apply substitutions
        processed = query_text

        for param, value in subs.items():
            # Handle different placeholder formats
            patterns = [
                f"[{param}]",  # Standard: [YEAR]
                f":{param}",   # Alternative: :YEAR
                f"${param}",   # Alternative: $YEAR
            ]

            for pattern in patterns:
                if pattern in processed:
                    processed = processed.replace(pattern, value)

        # Post-processing: Remove template artifacts like "100 select 100" and trailing "100;"
        # Remove pattern: " 100 select 100" at start of query
        processed = re.sub(r'^\s*100\s+select\s+100\s+', 'select ', processed, flags=re.MULTILINE)
        # Remove pattern: "100  select 100" (double space variant)
        processed = re.sub(r'^\s*100\s\s+select\s+100\s+', 'select ', processed, flags=re.MULTILINE)
        # Remove standalone "100" lines
        processed = re.sub(r'^\s*100\s*$', '', processed, flags=re.MULTILINE)
        # Remove "100;" at end
        processed = re.sub(r'\s+100\s*;', ';', processed)

        return processed

    def process_query_file(self, query_file: Path, custom_subs: Dict[str, str] = None) -> str:
        """
        Load query from DuckDB pre-processed file (no substitution needed).

        Args:
            query_file: Path to query file
            custom_subs: Optional custom substitutions (ignored, for compatibility)

        Returns:
            Query text ready for execution
        """
        with open(query_file, 'r') as f:
            query_text = f.read()

        # DuckDB queries are already processed - just return as-is
        return query_text.strip()

    def validate_query(self, query_text: str) -> Tuple[bool, List[str]]:
        """
        Validate that all parameters have been substituted.

        Args:
            query_text: Processed query text

        Returns:
            Tuple of (is_valid, list of remaining placeholders)
        """
        # Find remaining placeholders
        placeholders = []

        # Check for bracket placeholders [PARAM]
        bracket_pattern = r'\[([A-Z0-9_.]+)\]'
        brackets = re.findall(bracket_pattern, query_text)
        placeholders.extend(brackets)

        # Check for colon placeholders :PARAM
        colon_pattern = r':([A-Z0-9_]+)'
        colons = re.findall(colon_pattern, query_text)
        placeholders.extend(colons)

        # Check for dollar placeholders $PARAM
        dollar_pattern = r'\$([A-Z0-9_]+)'
        dollars = re.findall(dollar_pattern, query_text)
        placeholders.extend(dollars)

        is_valid = len(placeholders) == 0
        return is_valid, placeholders

    def get_query_metadata(self, query_num: int) -> Dict[str, Any]:
        """
        Get metadata for a specific query.

        Args:
            query_num: Query number (1-99)

        Returns:
            Dictionary with query metadata
        """
        import json

        metadata_file = Path(__file__).parent.parent.parent / "configs" / "queries" / "tpcds" / "queries_metadata.json"

        with open(metadata_file) as f:
            all_metadata = json.load(f)

        query_key = f"q{query_num}"

        return {
            "query_number": query_num,
            "query_key": query_key,
            "details": all_metadata["query_details"].get(query_key, {}),
            "categories": self._find_query_categories(query_num, all_metadata),
            "features": self._find_query_features(query_num, all_metadata),
        }

    def _find_query_categories(self, query_num: int, metadata: Dict) -> List[str]:
        """Find which categories a query belongs to."""
        categories = []

        for cat_name, cat_data in metadata.get("categories", {}).items():
            if query_num in cat_data.get("queries", []):
                categories.append(cat_name)

        for tier_name, tier_data in metadata.get("tiers", {}).items():
            if query_num in tier_data.get("queries", []):
                categories.append(tier_name)

        return categories

    def _find_query_features(self, query_num: int, metadata: Dict) -> List[str]:
        """Find which features a query uses."""
        features = []

        for feature_name, feature_data in metadata.get("features", {}).items():
            if query_num in feature_data.get("queries", []):
                features.append(feature_name)

        return features


def main():
    """Test query processor."""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python tpcds_query_processor.py <query_number>")
        print("Example: python tpcds_query_processor.py 3")
        sys.exit(1)

    query_num = int(sys.argv[1])
    query_file = Path(f"/opt/spark/configs/queries/tpcds/q{query_num:02d}.sql")

    if not query_file.exists():
        print(f"Error: Query file not found: {query_file}")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  TPC-DS Query Processor - Q{query_num}")
    print(f"{'='*60}\n")

    processor = TPCDSQueryProcessor(scale_factor=1)

    # Get metadata
    metadata = processor.get_query_metadata(query_num)
    print(f"📋 Metadata:")
    print(f"   Categories: {', '.join(metadata['categories'])}")
    print(f"   Features: {', '.join(metadata['features'])}")
    print()

    # Process query
    print(f"🔄 Processing query...")
    processed_query = processor.process_query_file(query_file)

    # Validate
    is_valid, remaining = processor.validate_query(processed_query)

    if is_valid:
        print(f"✅ Query processed successfully (all parameters substituted)")
    else:
        print(f"⚠️  Warning: {len(remaining)} parameters not substituted:")
        for param in remaining:
            print(f"   - {param}")

    print(f"\n{'='*60}")
    print("PROCESSED QUERY:")
    print(f"{'='*60}\n")
    print(processed_query[:1000])  # Print first 1000 chars
    if len(processed_query) > 1000:
        print("\n... (truncated)")


if __name__ == "__main__":
    main()
