package Chromosome;

import java.util.Random;

// Genetic Algorithm Node
public class Chromosome {
	// The chromo
	static char[] ltable = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '-', '*', '/' };
	static int chromoLen = 5;
	static double crossRate;
	static double mutRate;
	public StringBuffer chromo = new StringBuffer(chromoLen * 4);
	public StringBuffer decodeChromo = new StringBuffer(chromoLen * 4);
	static Random rand = new Random();
	
	public double score;
	public int total;

	// Constructor that generates a random
	public Chromosome(int target,double crossRate,double mutRate) {
		this.crossRate = crossRate;
		this.mutRate = mutRate;
		// Create the full buffer
		for (int y = 0; y < chromoLen; y++) {
			// What's the current length
			int pos = chromo.length();

			// Generate a random binary integer
			String binString = Integer.toBinaryString(rand.nextInt(ltable.length));
			int fillLen = 4 - binString.length();

			// Fill to 4
			for (int x = 0; x < fillLen; x++)
				chromo.append('0');

			// Append the chromo
			chromo.append(binString);
		}

		// Score the new cromo
		scoreChromo(target);
	}

	public Chromosome(StringBuffer chromo) {
		this.chromo = chromo;
	}

	// Decode the string
	public final String decodeChromo() {

		// Create a buffer
		decodeChromo.setLength(0);

		// Loop throught the chromo
		for (int x = 0; x < chromo.length(); x += 4) {
			// Get the
			int idx = Integer.parseInt(chromo.substring(x, x + 4), 2);
			if (idx < ltable.length)
				decodeChromo.append(ltable[idx]);
		}
System.out.println(decodeChromo.toString());
		// Return the string
		return decodeChromo.toString();
	}

	// Scores this chromo
	public final void scoreChromo(int target) {
		total = addUp();
		if (total == target)
			score = 0;
		score = (double) 1 / (target - total);
	}

	// Crossover bits
	public final void crossOver(Chromosome other) {

		// Should we cross over?
		if (rand.nextDouble() > crossRate)
			return;

		// Generate a random position
		int pos = rand.nextInt(chromo.length());

		// Swap all chars after that position
		for (int x = pos; x < chromo.length(); x++) {
			// Get our character
			char tmp = chromo.charAt(x);

			// Swap the chars
			chromo.setCharAt(x, other.chromo.charAt(x));
			other.chromo.setCharAt(x, tmp);
		}
	}

	// Mutation
	public final void mutate() {
		for (int x = 0; x < chromo.length(); x++) {
			if (rand.nextDouble() <= mutRate)
				chromo.setCharAt(x, (chromo.charAt(x) == '0' ? '1' : '0'));
		}
	}

	// Add up the contents of the decoded chromo
	public final int addUp() {

		// Decode our chromo
		String decodedString = decodeChromo();

		// Total
		int tot = 0;

		// Find the first number
		int ptr = 0;
		while (ptr < decodedString.length()) {
			char ch = decodedString.charAt(ptr);
			if (Character.isDigit(ch)) {
				tot = ch - '0';
				ptr++;
				break;
			} else {
				ptr++;
			}
		}

		// If no numbers found, return
		if (ptr == decodedString.length())
			return 0;

		// Loop processing the rest
		boolean num = false;
		char oper = ' ';
		while (ptr < decodedString.length()) {
			// Get the character
			char ch = decodedString.charAt(ptr);

			// Is it what we expect, if not - skip
			if (num && !Character.isDigit(ch)) {
				ptr++;
				continue;
			}
			if (!num && Character.isDigit(ch)) {
				ptr++;
				continue;
			}

			// Is it a number
			if (num) {
				switch (oper) {
				case '+': {
					tot += (ch - '0');
					break;
				}
				case '-': {
					tot -= (ch - '0');
					break;
				}
				case '*': {
					tot *= (ch - '0');
					break;
				}
				case '/': {
					if (ch != '0')
						tot /= (ch - '0');
					break;
				}
				}
			} else {
				oper = ch;
			}

			// Go to next character
			ptr++;
			num = !num;
		}

		return tot;
	}

	public final boolean isValid() {

		// Decode our chromo
		String decodedString = decodeChromo();

		boolean num = true;
		for (int x = 0; x < decodedString.length(); x++) {
			char ch = decodedString.charAt(x);

			// Did we follow the num-oper-num-oper-num patter
			if (num == !Character.isDigit(ch))
				return false;

			// Don't allow divide by zero
			if (x > 0 && ch == '0' && decodedString.charAt(x - 1) == '/')
				return false;

			num = !num;
		}

		// Can't end in an operator
		if (!Character.isDigit(decodedString.charAt(decodedString.length() - 1)))
			return false;

		return true;
	}
}
