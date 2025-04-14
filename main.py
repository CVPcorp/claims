import subprocess

def run_script(script_name):
    print(f"\nRunning {script_name}...")
    result = subprocess.run(['python', script_name], capture_output=True, text=True)
    print("Output:")
    print(result.stdout)
    if result.returncode != 0:
        print(f"Error running {script_name}:")
        print(result.stderr)
        exit(1)
    print(f"{script_name} completed successfully.")

if __name__ == "__main__":
    scripts = [
        "create_claims_db.py",
        "state_data_import.py",
        "gender_data_import.py",
        "claims_data_import.py",
        "bene_data_import.py",
        "identify_readmissions.py",
        "calc_readmission_rate.py",
        "chart.py",
        "icd_codes_import.py",
        "icd_description_import.py",
        "plotly_dashboard.py"
    ]

    for script in scripts:
        run_script(script)

    print("\nAll scripts have been executed successfully.")
