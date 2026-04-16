CREATE TABLE departments(
    dep_id INT AUTO_INCREMENT PRIMARY KEY,
    dep_name VARCHAR(100) NOT NULL,
    dep_location VARCHAR(100)
);

CREATE TABLE employees(
    emp_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    gender ENUM('Male', 'Female', 'Others'),
    age INT CHECK (age BETWEEN 18 AND 70),
    dep_id INT NOT NULL,
    salary DECIMAL(10, 2) CHECK (salary >= 0),
    join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    Foreign Key (dep_id) REFERENCES departments(dep_id) ON DELETE CASCADE
);

CREATE TABLE customers(
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    city VARCHAR(100),
    signup_date VARCHAR(100)
);

CREATE TABLE products(
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(100),
    price VARCHAR(50),
    stock VARCHAR(50),
    added_date VARCHAR(100)
);

CREATE TABLE orders(
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    quantity INT CHECK (quantity >= 0),
    order_date DATE,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE
);
