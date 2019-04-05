package com.sparktuts.poc;

public class IntegerWithSquare {
    private int originalNumber;
    private double squareRoot;

    public IntegerWithSquare(int i) {
        this.originalNumber = i;
        this.squareRoot = Math.sqrt(i);
    }
}
