public interface Data_size {
    int ID_OFFSET=0;
    int ID_SIZE =4;
    int DATETIME_OFFEST=4;
    int DATETIME_SIZE=22;
    int YEAR_OFFSET=26;
    int YEAR_SIZE=4;
    int MONTH_OFFSET=30;
    int MONTH_SIZE=10;
    int DATE_OFFSET=40;
    int DATE_SIZE=4;
    int DAY_OFFSET=44;
    int DAY_SIZE=10;
    int TIME_OFFSET=54;
    int TIME_SIZE=4;
    int SENSORID_OFFSET=58;
    int SENSORID_SIZE=4;
    int SENSORNAME_OFFSET=62;
    int SENSORNAME_SIZE=40;
    int HOURLYCOUNT_OFFSET=102;
    int HOURLYCOUNT_SIZE=4;
    int ROW_SIZE = ID_SIZE+DATETIME_SIZE+YEAR_SIZE+MONTH_SIZE+DATE_SIZE+DAY_SIZE+TIME_SIZE+SENSORID_SIZE+SENSORNAME_SIZE+HOURLYCOUNT_SIZE;
    String heapfile_name="heap.";
   // String CSVFile_Name="";


}
