package mapreduce;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CommodityYearWritable implements Writable {

    private String commodity;
    private Integer year;

    @Override
    public void readFields(DataInput in) throws IOException {
        commodity = in.readUTF();
        year = Integer.parseInt(in.readUTF());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(commodity);
        out.writeUTF(String.valueOf(year));
    }

    public String getCommodity() { return commodity; }

    public void setCommodity(String commodity) { this.commodity = commodity; }

    public Integer getYear() { return year; }

    public void setYear(Integer flow) { this.year = year; }

    public CommodityYearWritable() {
    }

    public CommodityYearWritable(String commodity, Integer year) {
        this.commodity = commodity;
        this.year = year;
    }

}
