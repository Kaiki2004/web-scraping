-- Criação do banco de dados
CREATE DATABASE ecommerce_scraping;
USE ecommerce_scraping;

-- ================================
-- Tabela Fornecedores
-- ================================
CREATE TABLE Fornecedores (
    id_fornecedor INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    code VARCHAR(100) UNIQUE
);

-- ================================
-- Tabela Seller
-- ================================
CREATE TABLE Seller (
    id_seller INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    fk_fornecedor INT NOT NULL,
    FOREIGN KEY (fk_fornecedor) REFERENCES Fornecedores(id_fornecedor)
);

-- ================================
-- Tabela Products
-- ================================
CREATE TABLE Products (
    id_product INT AUTO_INCREMENT PRIMARY KEY,
    brand VARCHAR(255) NOT NULL,
    code VARCHAR(100) UNIQUE,
    model VARCHAR(255),
    variante VARCHAR(255)
);

-- ================================
-- Tabela List
-- ================================
CREATE TABLE List (
    id_list INT AUTO_INCREMENT PRIMARY KEY,
    url VARCHAR(500),
    price DECIMAL(10,2),
    avaliacao DECIMAL(3,2),
    frete_price DECIMAL(10,2),
    prazo_entrega VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    fk_product INT NOT NULL,
    fk_seller INT NOT NULL,
    fk_fornecedor INT NOT NULL,
    
    FOREIGN KEY (fk_product) REFERENCES Products(id_product),
    FOREIGN KEY (fk_seller) REFERENCES Seller(id_seller),
    FOREIGN KEY (fk_fornecedor) REFERENCES Fornecedores(id_fornecedor)
);
