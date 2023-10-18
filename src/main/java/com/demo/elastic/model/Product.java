package com.demo.elastic.model;

public class Product {
    private String name;
    private String category;
    private double price;

    private int amount;

    public Product(String name, String category, double price) {
        this.name = name;
        this.category = category;
        this.price = price;
    }

    public Product(String name, String category, double price, int amount) {
        this.name = name;
        this.category = category;
        this.price = price;
        this.amount = amount;
    }

    public Product(){}

    public String getName() {
        return name;
    }

    public String getCategory() {
        return category;
    }

    public double getPrice() {
        return price;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "Product{" +
                "name='" + name + '\'' +
                ", category='" + category + '\'' +
                ", price=" + price +
                ", amount=" + amount +
                '}';
    }
}
