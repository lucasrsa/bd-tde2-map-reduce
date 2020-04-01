package mapreduce;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CommodityCostWeightWritable implements Writable {

    private Float cost;
    private Float weight;

    @Override
    public void readFields(DataInput in) throws IOException {
        cost = Float.parseFloat(in.readUTF());
        weight = Float.parseFloat(in.readUTF());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(cost));
        out.writeUTF(String.valueOf(weight));
    }

    public Float getCost() { return cost; }

    public void setCost(Float cost) { this.cost = cost; }

    public Float getWeight() { return weight; }

    public void setWeight(Float weight) { this.weight = weight; }

    public CommodityCostWeightWritable() {
    }

    public CommodityCostWeightWritable(Float cost, Float weight) {
        this.cost = cost;
        this.weight = weight;
    }

}
