package mapreduce;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CommodityYearWritableComparable implements WritableComparable {

    private String commodity;
    private String year;

//    @Override
    public void readFields(DataInput in) throws IOException {
        commodity = in.readUTF();
        year = in.readUTF();
    }

//    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(commodity);
        out.writeUTF(year);
    }

    @Override
    public int compareTo(Object o) {
        if ( o instanceof CommodityYearWritableComparable ) {
            CommodityYearWritableComparable cy = (CommodityYearWritableComparable) o;
            return ( this.commodity.compareTo(cy.commodity) + this.year.compareTo(cy.year) );
        }
        return -1;
    }

    @Override
    public boolean equals(Object o) {
        return ( this.compareTo(o) == 0 );
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.commodity, this.year);
    }

    @Override
    public String toString() {
        return this.commodity + "\t" + this.year;
    }

    public String getCommodity() { return commodity; }

    public void setCommodity(String commodity) { this.commodity = commodity; }

    public String getYear() { return year; }

    public void setYear(String flow) { this.year = year; }

    public CommodityYearWritableComparable() {
    }

    public CommodityYearWritableComparable(String commodity, String year) {
        this.commodity = commodity;
        this.year = year;
    }

}
