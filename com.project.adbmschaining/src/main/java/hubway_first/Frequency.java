package hubway_first;

import java.util.Comparator;

public class Frequency {

	private int sum;

	public int getSum() {

		return sum;

	}

	public void setSum(int sum) {

		this.sum = sum;

	}

	public Frequency(int sum) {

		super();

		this.sum = sum;

	}
}

class FrequencyComp implements Comparator<Frequency> {

	public int compare(Frequency e1, Frequency e2) {

		if (e1.getSum() > e2.getSum()) {

			return 1;

		} else {

			return -1;

		}

	}

}