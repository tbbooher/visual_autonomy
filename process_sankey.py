import pandas as pd
import logging
from sqlalchemy import text

def build_sankey_rows(program_name, current_level, df, sankey_rows, visited, id_to_name, levels_dict):
    """Recursive function to determine levels and build source-target relationships for the Sankey diagram."""
    if current_level > 6:  # Prevent going beyond 6 levels
        return

    program_rows = df[df['Program Name'] == program_name]

    if program_rows.empty or program_name in visited:
        logging.info(f"No further dependencies for {program_name} or already visited.")
        levels = [None] * (6 - current_level) + [program_name] + [None] * (current_level - 1)
        levels = levels[-6:]  # Keep only 6 levels
        levels_dict[program_name] = levels
        sankey_rows.append({
            'source': program_name,
            'target': program_name,
            'level': current_level,
            'value': program_rows['Total Funding (m)'].sum() if not program_rows.empty else 0,
            'theme': program_rows['Theme'].iloc[0] if not program_rows.empty else None,
            'total_funding': program_rows['Total Funding (m)'].sum() if not program_rows.empty else 0,
            'start_year': program_rows['Start Year'].min() if not program_rows.empty else None,
            'end_year': program_rows['End Year'].max() if not program_rows.empty else None,
            **{f'level_{i+1}': level_name for i, level_name in enumerate(levels, start=1) if level_name is not None}
        })
        return

    visited.add(program_name)
    levels = [None] * (6 - current_level) + [program_name] + [None] * (current_level - 1)
    levels = levels[-6:]  # Ensure exactly 6 levels
    levels_dict[program_name] = levels

    for _, row in program_rows.iterrows():
        dependencies = str(row['Dependency']).split(',') if pd.notna(row['Dependency']) else []

        if not dependencies or dependencies == ['']:
            logging.info(f"No dependencies for {row['Program Name']}. Terminating at self.")
            levels = levels_dict[program_name][:6-current_level] + [row['Program Name']] + [None] * (6 - current_level)
            levels = levels[-6:]  # Ensure only 6 levels
            levels_dict[row['Program Name']] = levels
            sankey_rows.append({
                'source': row['Program Name'],
                'target': row['Program Name'],
                'level': current_level,
                'value': row['Total Funding (m)'],
                'theme': row['Theme'],
                'total_funding': row['Total Funding (m)'],
                'start_year': row['Start Year'],
                'end_year': row['End Year'],
                **{f'level_{i+1}': level_name for i, level_name in enumerate(levels, start=1) if level_name is not None}
            })
        else:
            for dependency in dependencies:
                dependency = dependency.strip()
                if dependency.isdigit():
                    dependency_name = id_to_name.get(dependency, None)
                    if dependency_name:
                        levels = levels_dict[program_name][:6-current_level] + [dependency_name] + [None] * (current_level - 1)
                        levels = levels[-6:]  # Ensure only 6 levels
                        levels_dict[dependency_name] = levels
                        sankey_rows.append({
                            'source': dependency_name,
                            'target': row['Program Name'],
                            'level': current_level,
                            'value': row['Total Funding (m)'],
                            'theme': row['Theme'],
                            'total_funding': row['Total Funding (m)'],
                            'start_year': row['Start Year'],
                            'end_year': row['End Year'],
                            **{f'level_{i+1}': level_name for i, level_name in enumerate(levels, start=1) if level_name is not None}
                        })
                        # Stop recursion at transition to prevent extra levels
                        return
                    else:
                        logging.warning(f"Dependency ID '{dependency}' not found in Program Names. Skipping.")
                else:
                    logging.warning(f"Dependency '{dependency}' is not a valid ID. Skipping.")

def create_and_populate_sankey_data_table(df, engine):
    try:
        # Drop the sankey_data table to completely replace it
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS sankey_data CASCADE"))
            conn.commit()

        # Create the sankey_data table with up to 6 levels
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS sankey_data (
                    source TEXT,
                    target TEXT,
                    level INT,
                    value DOUBLE PRECISION,
                    theme TEXT,
                    total_funding DOUBLE PRECISION,
                    start_year DATE,
                    end_year DATE,
                    level_1 TEXT,
                    level_2 TEXT,
                    level_3 TEXT,
                    level_4 TEXT,
                    level_5 TEXT,
                    level_6 TEXT
                )
            """))
            conn.commit()

        # Create a mapping from program IDs to program names
        id_to_name = pd.Series(df['Program Name'].values, index=df['ID']).to_dict()

        sankey_rows = []
        levels_dict = {}

        # Build the sankey rows starting with each unique program
        unique_programs = df['Program Name'].unique()
        for program in unique_programs:
            build_sankey_rows(program, 1, df, sankey_rows, set(), id_to_name, levels_dict)

        # Create DataFrame from sankey_rows and insert into the database
        sankey_df = pd.DataFrame(sankey_rows).drop_duplicates()

        # Drop columns with all nulls
        sankey_df = sankey_df.dropna(axis=1, how='all')

        sankey_df.to_sql('sankey_data', engine, if_exists='append', index=False, method='multi', chunksize=1000)
        logging.info("Sankey data table populated successfully.")

    except Exception as e:
        logging.error(f"An error occurred while creating or populating sankey_data table: {e}")
