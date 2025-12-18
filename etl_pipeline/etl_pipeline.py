# usage: python etl_pipeline.py

import pandas as pd
import numpy as np
import logging
from pathlib import Path

# Letś configure the logs:
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PropelETLPipeline:
    """ETL Pipeline for Propel Data Processing"""

    def __init__(self, input_file, output_dir='./output'):

        """Initialize the ETL pipeline with input file and output directory
        
        Args:
        Input_file (str): Path to the input Excel file.
        output_dir (str): Path to the output directory.
        """
        self.input_file = Path(input_file)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.df_raw = None
        self.df_clean = None
        self.summaries = {}
        
        logger.info(f"ETL Pipeline initialized")
        logger.info(f"Input: {self.input_file}")
        logger.info(f"Output directory: {self.output_dir}")
    
    def load_raw_data(self):
        """Step 1: Load raw Excel data"""
        logger.info("LOADING RAW DATA")

        
        try:
            self.df_raw = pd.read_excel(self.input_file, sheet_name='Sheet1')
            logger.info(f"✓ Data loaded: {self.df_raw.shape[0]} rows × {self.df_raw.shape[1]} columns")
            logger.info(f"Columns: {', '.join(self.df_raw.columns)}")
            return self
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise
    
    def clean_data(self):
        """Step 2: Clean the raw data"""
        logger.info("CLEANING DATA")

        self.df_clean = self.df_raw.copy()

        # Standardize country names
        logger.info("\n2.1 Standardizing country names...")
        countries_before = self.df_clean['Country'].nunique()
        self.df_clean['Country'] = self.df_clean['Country'].str.strip().str.capitalize()
        countries_after = self.df_clean['Country'].nunique()
        logger.info(f"✓ Countries: {countries_before} → {countries_after}")
        logger.info(f"  Values: {sorted(self.df_clean['Country'].unique())}")

        # 2.2: Convert dates to datetime
        logger.info("\n2.2 Converting dates to datetime...")
        date_columns = ['Registration_Date', 'Campaign_Start_Date', 'Campaign_End_Date', 'Attendance_Date']
        for col in date_columns:
            self.df_clean[col] = pd.to_datetime(self.df_clean[col], errors='coerce')
        logger.info(f"{len(date_columns)} date columns converted")
        
        # 2.3: Standardize boolean columns
        logger.info("\n2.3 Standardizing boolean columns...")
        self.df_clean['Registered'] = self.df_clean['Registered'].apply(
            lambda x: True if str(x).lower() in ['yes', 'true', 1, '1'] else False
        )
        self.df_clean['Attended'] = self.df_clean['Attended'].apply(
            lambda x: True if str(x).lower() in ['yes', 'true', 1, '1'] else False
        )
        logger.info(f"Boolean columns standardized")
        
        # 2.4: Create derived fields
        logger.info("\n2.4 Creating derived fields...")
        self.df_clean['Registration_Month'] = self.df_clean['Registration_Date'].dt.strftime('%B %Y')
        self.df_clean['Attendance_Month'] = self.df_clean['Attendance_Date'].dt.strftime('%B %Y')
        self.df_clean['Campaign_Duration_Days'] = (
            self.df_clean['Campaign_End_Date'] - self.df_clean['Campaign_Start_Date']
        ).dt.days

        # Create Attendance_Status
        def get_attendance_status(row):
            if pd.notna(row['Attendance_Date']):
                return 'Attended'
            elif row['Registered']:
                return 'Registered - No Attendance'
            else:
                return 'Not Registered'
        
        self.df_clean['Attendance_Status'] = self.df_clean.apply(get_attendance_status, axis=1)
        logger.info(f"Derived fields created: Registration_Month, Attendance_Month, Campaign_Duration_Days, Attendance_Status")
        
        # 2.5: Data quality metrics
        logger.info("\n2.5 Data quality metrics:")
        logger.info(f"Total records: {len(self.df_clean)}")
        logger.info(f"Registered: {self.df_clean['Registered'].sum()} ({self.df_clean['Registered'].sum()/len(self.df_clean)*100:.1f}%)")
        logger.info(f"Attended: {self.df_clean['Attendance_Date'].notna().sum()} ({self.df_clean['Attendance_Date'].notna().sum()/len(self.df_clean)*100:.1f}%)")
        
        status_dist = self.df_clean['Attendance_Status'].value_counts()
        logger.info(f"Attendance Status Distribution:")
        for status, count in status_dist.items():
            logger.info(f" - {status}: {count} ({count/len(self.df_clean)*100:.1f}%)")
        
        logger.info("\nDATA CLEANING COMPLETE")
        return self
    
    def generate_summaries(self):
        """Step 3: Generate summary statistics"""
        logger.info("\n" + "=" * 100)
        logger.info("STEP 3: GENERATING SUMMARIES")
        logger.info("=" * 100)
        
        # Campaign summary
        logger.info("\n3.1 Campaign Summary...")
        self.summaries['campaigns'] = self.df_clean.groupby('Campaign_Name').agg({
            'Registered': 'sum',
            'Attendance_Date': 'count'
        }).rename(columns={'Attendance_Date': 'Attended'})
        logger.info(f"✓ {len(self.summaries['campaigns'])} campaigns summarized")
        
        # Country summary
        logger.info("\n3.2 Country Summary...")
        self.summaries['countries'] = self.df_clean.groupby('Country').size().to_frame(name='Count')
        logger.info(f"✓ {len(self.summaries['countries'])} countries identified")
        
        # Monthly summary
        logger.info("\n3.3 Monthly Summary...")
        self.summaries['monthly'] = self.df_clean.groupby('Registration_Month').size().to_frame(name='Registrations')
        logger.info(f"✓ Monthly data aggregated")
        
        return self
    
    def export_data(self):
        """Step 4: Export clean data and summaries"""
        logger.info("\n" + "=" * 100)
        logger.info("STEP 4: EXPORTING DATA & SUMMARIES")
        logger.info("=" * 100)
        
        # Prepare export dataframe (convert booleans to string)
        df_export = self.df_clean.copy()
        df_export['Registered'] = df_export['Registered'].astype(str)
        df_export['Attended'] = df_export['Attended'].astype(str)
        
        # 4.1: Export clean dataset
        logger.info("\n4.1 Exporting clean dataset...")
        
        # Excel
        excel_path = self.output_dir / 'propel_cleaned_data.xlsx'
        df_export.to_excel(excel_path, index=False, sheet_name='Data')
        logger.info(f"Excel: {excel_path} ({excel_path.stat().st_size / 1024:.1f} KB)")
        
        # CSV
        csv_path = self.output_dir / 'propel_cleaned_data.csv'
        df_export.to_csv(csv_path, index=False)
        logger.info(f"CSV: {csv_path} ({csv_path.stat().st_size / 1024:.1f} KB)")

        return self
    
    def run(self):
        """Execute the complete ETL pipeline"""
        try:
            self.load_raw_data()
            self.clean_data()
            self.generate_summaries()
            self.export_data()
            logger.info("\nETL PIPELINE COMPLETED SUCCESSFULLY")
            return True
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            return False
    
if __name__ == '__main__':
    import sys
    from pathlib import Path
    
    # Get input and output paths from environment or arguments
    input_file = Path('/data/input/data propel.xlsx')  # Default Docker path
    output_dir = Path('/data/output')
    
    # Allow override via command line
    if len(sys.argv) > 1:
        input_file = Path(sys.argv[1])
    if len(sys.argv) > 2:
        output_dir = Path(sys.argv[2])
    
    # Run ETL pipeline
    pipeline = PropelETLPipeline(input_file, output_dir)
    success = pipeline.run()
    
    sys.exit(0 if success else 1)
