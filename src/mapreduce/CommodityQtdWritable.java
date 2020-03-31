package mapreduce;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CommodityQtdWritable implements Writable {

    private String commodity;
    private Float quantity;

    @Override
    public void readFields(DataInput in) throws IOException {
        commodity = in.readUTF();
        quantity = Float.parseFloat(in.readUTF());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(commodity);
        out.writeUTF(String.valueOf(quantity));
    }

    public String getCommodity() { return commodity; }

    public void setCommodity(String commodity) { this.commodity = commodity; }

    public Float getQuantity() { return quantity; }

    public void setQuantity(Float flow) { this.quantity = quantity; }

    public CommodityQtdWritable() {
    }

    public CommodityQtdWritable(String commodity, Float quantity) {
        this.commodity = commodity;
        this.quantity = quantity;
    }

}
