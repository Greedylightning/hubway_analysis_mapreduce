package hubway_first;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MedianSDCustomWritable implements Writable {

	double standardDeviation;
	double median;
	double average;

	public double getAverage() {
		return average;
	}

	public void setAverage(double average) {
		this.average = average;
	}

	public double getMedian() {
		return median;
	}

	public void setMedian(double median) {
		this.median = median;
	}

	public double getStandardDeviation() {
		return standardDeviation;
	}

	public void setStandardDeviation(double standardDeviation) {
		this.standardDeviation = standardDeviation;
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(median);
		out.writeDouble(standardDeviation);
		out.writeDouble(average);

	}

	public void readFields(DataInput in) throws IOException {
		standardDeviation = in.readDouble();
		median = in.readDouble();
		average = in.readDouble();

	}

	@Override
	public String toString() {
		return (this.getMedian() + "\t" + this.getStandardDeviation() + "\t" + this.getAverage());
	}

}
