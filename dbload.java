import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class dbload implements Data_size {
    byte [] row=new byte[ROW_SIZE];
    byte [] page;

    public static void main(String [] args)
    {
        dbload db = new dbload();
        if(args.length==3)
        {
            if(args[0].equals("-p")&&isValidSize(args[1]))
            {
                long startLoadTime = System.currentTimeMillis();
                db.Load_Data_File(args[2],Integer.parseInt(args[1]),db);
                long endLoadTime = System.currentTimeMillis();
                System.out.println("Total Loading time: " + (endLoadTime - startLoadTime) + "ms");
                db.saveStats("\nTotal Loading time: " + (endLoadTime - startLoadTime) + "ms\n\n");

            }
            else
            {
                System.err.println("Invalid Arguments \t the format must be  \n java dbload -p pagesize datafile ");
            }
        }
        else
        {
            System.err.println("Missing Argument \t the format must be  \n java dbload -p pagesize datafile");
        }


    }

    private void saveStats(String s) {
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter("stdout", true));

            writer.write("dbload: " + s);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void Load_Data_File(String CSV_FILE, int pagesize,dbload db) {
        try {

            File file = new File(CSV_FILE);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line = "";
            br.readLine();
            int rowcount=0;
            int pagesize_remaining=0;
            int page_count=1;
            page = new byte[pagesize];
            int numberRowPerPage=pagesize/ROW_SIZE;
            System.out.println("rowperpage"+numberRowPerPage);
            int PAGE_OFFSET=0;
            String fill = "/v";
            int fillcharacterlenght=pagesize-numberRowPerPage*ROW_SIZE;

            OutputStream output
                    = new FileOutputStream(heapfile_name+pagesize);
            while ((line = br.readLine()) != null)
            {
                String [] rowdata = line.split(",",-1);
                // 1 FOR ID
                int id = Integer.parseInt(rowdata[0]);
                ByteBuffer ID = ByteBuffer.allocate(ID_SIZE);
                byte [] id_InBYTE=ID.putInt(id).array();
                // 2 FOR DATE TIME
                String Date_Time = rowdata[1];
                //ByteBuffer Date_TIME = ByteBuffer.allocate(DATE_SIZE);
                byte [] DateTime_BYTE = Date_Time.getBytes(StandardCharsets.UTF_8);
                //db.fill_gaps(DateTime_BYTE,DATETIME_SIZE);
                // 3 FOR YEAR
                int year = Integer.parseInt(rowdata[2]);
                ByteBuffer YEAR = ByteBuffer.allocate(YEAR_SIZE);
                byte [] Year_InBYTE=YEAR.putInt(year).array();
                // 4 FOR MONTH
                String Month = rowdata[3];
                byte [] Month_BYTE = Month.getBytes(StandardCharsets.UTF_8);
               // db.fill_gaps(Month_BYTE,MONTH_SIZE);
                // 5 FOR DATE
                int date = Integer.parseInt(rowdata[4]);
                ByteBuffer DATE = ByteBuffer.allocate(DATE_SIZE);
                byte [] Date_InBYTE=DATE.putInt(date).array();
                // 6 FOR DAY
                String Day = rowdata[5];
                byte [] Day_BYTE = Day.getBytes(StandardCharsets.UTF_8);
               // db.fill_gaps(Day_BYTE,DAY_SIZE);
                // 7 FOR TIME
                int time = Integer.parseInt(rowdata[6]);
                ByteBuffer TIME = ByteBuffer.allocate(DATE_SIZE);
                byte [] Time_InBYTE=TIME.putInt(time).array();
                // 8 FOR SENSOR_ID
                int sensor_id = Integer.parseInt(rowdata[7]);
                ByteBuffer SENSORID = ByteBuffer.allocate(DATE_SIZE);
                byte [] SensorID_InBYTE=SENSORID.putInt(sensor_id).array();
                //9 FOR SENSOR_NAME
                String sensor_Name = rowdata[8];
                byte [] SENSORNAME_BYTE = sensor_Name.getBytes(StandardCharsets.UTF_8);
               // db.fill_gaps(SENSORNAME_BYTE,SENSORNAME_SIZE);
                // 10 FOR HOURLY COUNT
                int hourly_count = Integer.parseInt(rowdata[7]);
                ByteBuffer HOURLY = ByteBuffer.allocate(DATE_SIZE);
                byte [] HourlyCount_InBYTE=HOURLY.putInt(hourly_count).array();
               db.copyRow(id_InBYTE,DateTime_BYTE,Year_InBYTE,Month_BYTE,Date_InBYTE,Day_BYTE,Time_InBYTE,SensorID_InBYTE,SENSORNAME_BYTE,HourlyCount_InBYTE);
               rowcount++;
              if(numberRowPerPage>0)
              {
                  //output.write(row);
                  db.GenaratePage(page,row,PAGE_OFFSET);
                  PAGE_OFFSET= db.UpdateOffset(PAGE_OFFSET);
                  numberRowPerPage--;
              }
//              else if(fillcharacterlenght>0)
//              {
//                  String st ="";
//                  for(int i=0;i<fillcharacterlenght;i++)
//                  {
//                       st =st+"#";
//                  }
//                  byte [] fillPage_Byte = st.getBytes(StandardCharsets.UTF_8);
//                  System.arraycopy(fillPage_Byte,0,page,PAGE_OFFSET,fillcharacterlenght);
//              }
              else {
                  page_count++;
                  output.write(page);
                  page=new byte[pagesize];
                  PAGE_OFFSET=0;
                  numberRowPerPage=pagesize/ROW_SIZE;
              }

            }
            output.close();
            System.out.println("number of record loaded "+rowcount);
            System.out.println("number of Pages created "+page_count);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    private void fill_gaps(byte[] fill_byte ,int array_size) {
//        int count;
//        int location= fill_byte.length;
//        String fill=null;
//        System.out.println("arraysize"+array_size);
//        if(fill_byte.length<array_size)
//        {
//
//            count= fill_byte.length;
//            while(count<array_size)
//            {
//            fill = fill+"#";
//            count++;
//            }
//            byte [] fillPage_Byte = fill.getBytes(StandardCharsets.UTF_8);
//            System.arraycopy(fillPage_Byte,0,fill_byte,location,fillPage_Byte.length);
//
//        }
//    }

    private int UpdateOffset(int page_offset) {
        return page_offset+ROW_SIZE;
    }

    private void GenaratePage(byte[] page, byte[] row,int PAGE_OFFSET) {
        System.arraycopy(row,0,page,PAGE_OFFSET,row.length);
        //System.out.println("page lenght"+page);
    }

    private void copyRow(byte[] id_inBYTE, byte[] dateTime_byte, byte[] year_inBYTE, byte[] month_byte, byte[] date_inBYTE, byte[] day_byte, byte[] time_inBYTE, byte[] sensorID_inBYTE, byte[] sensorname_byte, byte[] hourlyCount_inBYTE) {

        System.arraycopy(id_inBYTE,0,row,ID_OFFSET,ID_SIZE);
        System.arraycopy(dateTime_byte,0,row,DATETIME_OFFEST,dateTime_byte.length);
        System.arraycopy(year_inBYTE,0,row,YEAR_OFFSET,YEAR_SIZE);
        System.arraycopy(month_byte,0,row,MONTH_OFFSET,month_byte.length);
        System.arraycopy(date_inBYTE,0,row,DATE_OFFSET,DATE_SIZE);
        System.arraycopy(day_byte,0,row,DAY_OFFSET,date_inBYTE.length);
        System.arraycopy(time_inBYTE,0,row,TIME_OFFSET,TIME_SIZE);
        System.arraycopy(sensorID_inBYTE,0,row,SENSORID_OFFSET,SENSORID_SIZE);
        System.arraycopy(sensorname_byte,0,row,SENSORNAME_OFFSET,sensorname_byte.length);
        System.arraycopy(hourlyCount_inBYTE,0,row,HOURLYCOUNT_OFFSET,HOURLYCOUNT_SIZE);
        //TO DO FIX THE LENGHT SUCH THAT EACH ROW MUST BE 106;
    }

    private static boolean isValidSize(String arg) {
        boolean isvalidsize=false;
        try
        {
            Integer.parseInt(arg);
            isvalidsize=true;
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        return isvalidsize;
    }


}
