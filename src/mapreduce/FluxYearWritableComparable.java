package mapreduce;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class FluxYearWritableComparable implements WritableComparable {
    // Some data
    private String year;
    private String flux;

    //    @Override
    public void readFields(DataInput in) throws IOException {
        year = in.readUTF();
        flux = in.readUTF();
    }

    //    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(year);
        out.writeUTF(flux);
    }

    @Override
    public int compareTo(Object o) {
        if ( o instanceof FluxYearWritableComparable) {
            FluxYearWritableComparable cy = (FluxYearWritableComparable) o;
            return ( this.flux.compareTo(cy.flux) + this.year.compareTo(cy.year) );
        }
        return -1;
    }

    @Override
    public String toString() {
        return this.year + "\t" + this.flux;
    }

    public String getYear() { return year; }

    public void setYear(String year) { this.year = year; }

    public String getFlux() { return flux; }

    public void setFlux(String flux) { this.flux = flux; }

    public FluxYearWritableComparable() {
    }

    public FluxYearWritableComparable(String year, String flux) {
        this.year = year;
        this.flux = flux;
    }

}