SET UNIQUE_CHECKS=0;
SET FOREIGN_KEY_CHECKS=0;
SET SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

-- -----------------------------------------------------
-- Table `customers`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `customers` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `company` VARCHAR(50) NULL DEFAULT NULL,
  `last_name` VARCHAR(50) NULL DEFAULT NULL,
  `first_name` VARCHAR(50) NULL DEFAULT NULL,
  `email_address` VARCHAR(50) NULL DEFAULT NULL,
  `job_title` VARCHAR(50) NULL DEFAULT NULL,
  `business_phone` VARCHAR(25) NULL DEFAULT NULL,
  `home_phone` VARCHAR(25) NULL DEFAULT NULL,
  `mobile_phone` VARCHAR(25) NULL DEFAULT NULL,
  `fax_number` VARCHAR(25) NULL DEFAULT NULL,
  `address` LONGTEXT NULL DEFAULT NULL,
  `city` VARCHAR(50) NULL DEFAULT NULL,
  `state_province` VARCHAR(50) NULL DEFAULT NULL,
  `zip_postal_code` VARCHAR(15) NULL DEFAULT NULL,
  `country_region` VARCHAR(50) NULL DEFAULT NULL,
  `web_page` LONGTEXT NULL DEFAULT NULL,
  `notes` LONGTEXT NULL DEFAULT NULL,
  `attachments` LONGBLOB NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `city` (`city` ASC),
  INDEX `company` (`company` ASC),
  INDEX `first_name` (`first_name` ASC),
  INDEX `last_name` (`last_name` ASC),
  INDEX `zip_postal_code` (`zip_postal_code` ASC),
  INDEX `state_province` (`state_province` ASC))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `employees`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `employees` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `company` VARCHAR(50) NULL DEFAULT NULL,
  `last_name` VARCHAR(50) NULL DEFAULT NULL,
  `first_name` VARCHAR(50) NULL DEFAULT NULL,
  `email_address` VARCHAR(50) NULL DEFAULT NULL,
  `job_title` VARCHAR(50) NULL DEFAULT NULL,
  `business_phone` VARCHAR(25) NULL DEFAULT NULL,
  `home_phone` VARCHAR(25) NULL DEFAULT NULL,
  `mobile_phone` VARCHAR(25) NULL DEFAULT NULL,
  `fax_number` VARCHAR(25) NULL DEFAULT NULL,
  `address` LONGTEXT NULL DEFAULT NULL,
  `city` VARCHAR(50) NULL DEFAULT NULL,
  `state_province` VARCHAR(50) NULL DEFAULT NULL,
  `zip_postal_code` VARCHAR(15) NULL DEFAULT NULL,
  `country_region` VARCHAR(50) NULL DEFAULT NULL,
  `web_page` LONGTEXT NULL DEFAULT NULL,
  `notes` LONGTEXT NULL DEFAULT NULL,
  `attachments` LONGBLOB NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `city` (`city` ASC),
  INDEX `company` (`company` ASC),
  INDEX `first_name` (`first_name` ASC),
  INDEX `last_name` (`last_name` ASC),
  INDEX `zip_postal_code` (`zip_postal_code` ASC),
  INDEX `state_province` (`state_province` ASC))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `privileges`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `privileges` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `privilege_name` VARCHAR(50) NULL DEFAULT NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `employee_privileges`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `employee_privileges` (
  `employee_id` INT(11) NOT NULL,
  `privilege_id` INT(11) NOT NULL,
  PRIMARY KEY (`employee_id`, `privilege_id`),
  INDEX `employee_id` (`employee_id` ASC),
  INDEX `privilege_id` (`privilege_id` ASC),
  INDEX `privilege_id_2` (`privilege_id` ASC),
  CONSTRAINT `fk_employee_privileges_employees1`
    FOREIGN KEY (`employee_id`)
    REFERENCES `employees` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_employee_privileges_privileges1`
    FOREIGN KEY (`privilege_id`)
    REFERENCES `privileges` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `inventory_transaction_types`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `inventory_transaction_types` (
  `id` TINYINT(4) NOT NULL,
  `type_name` VARCHAR(50) NOT NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `shippers`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `shippers` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `company` VARCHAR(50) NULL DEFAULT NULL,
  `last_name` VARCHAR(50) NULL DEFAULT NULL,
  `first_name` VARCHAR(50) NULL DEFAULT NULL,
  `email_address` VARCHAR(50) NULL DEFAULT NULL,
  `job_title` VARCHAR(50) NULL DEFAULT NULL,
  `business_phone` VARCHAR(25) NULL DEFAULT NULL,
  `home_phone` VARCHAR(25) NULL DEFAULT NULL,
  `mobile_phone` VARCHAR(25) NULL DEFAULT NULL,
  `fax_number` VARCHAR(25) NULL DEFAULT NULL,
  `address` LONGTEXT NULL DEFAULT NULL,
  `city` VARCHAR(50) NULL DEFAULT NULL,
  `state_province` VARCHAR(50) NULL DEFAULT NULL,
  `zip_postal_code` VARCHAR(15) NULL DEFAULT NULL,
  `country_region` VARCHAR(50) NULL DEFAULT NULL,
  `web_page` LONGTEXT NULL DEFAULT NULL,
  `notes` LONGTEXT NULL DEFAULT NULL,
  `attachments` LONGBLOB NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `city` (`city` ASC),
  INDEX `company` (`company` ASC),
  INDEX `first_name` (`first_name` ASC),
  INDEX `last_name` (`last_name` ASC),
  INDEX `zip_postal_code` (`zip_postal_code` ASC),
  INDEX `state_province` (`state_province` ASC))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `orders_tax_status`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `orders_tax_status` (
  `id` TINYINT(4) NOT NULL,
  `tax_status_name` VARCHAR(50) NOT NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `orders_status`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `orders_status` (
  `id` TINYINT(4) NOT NULL,
  `status_name` VARCHAR(50) NOT NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `orders`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `orders` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `employee_id` INT(11) NULL DEFAULT NULL,
  `customer_id` INT(11) NULL DEFAULT NULL,
  `order_date` DATETIME NULL DEFAULT NULL,
  `shipped_date` DATETIME NULL DEFAULT NULL,
  `shipper_id` INT(11) NULL DEFAULT NULL,
  `ship_name` VARCHAR(50) NULL DEFAULT NULL,
  `ship_address` LONGTEXT NULL DEFAULT NULL,
  `ship_city` VARCHAR(50) NULL DEFAULT NULL,
  `ship_state_province` VARCHAR(50) NULL DEFAULT NULL,
  `ship_zip_postal_code` VARCHAR(50) NULL DEFAULT NULL,
  `ship_country_region` VARCHAR(50) NULL DEFAULT NULL,
  `shipping_fee` DECIMAL(19,4) NULL DEFAULT '0.0000',
  `taxes` DECIMAL(19,4) NULL DEFAULT '0.0000',
  `payment_type` VARCHAR(50) NULL DEFAULT NULL,
  `paid_date` DATETIME NULL DEFAULT NULL,
  `notes` LONGTEXT NULL DEFAULT NULL,
  `tax_rate` DOUBLE NULL DEFAULT '0',
  `tax_status_id` TINYINT(4) NULL DEFAULT NULL,
  `status_id` TINYINT(4) NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  INDEX `customer_id` (`customer_id` ASC),
  INDEX `customer_id_2` (`customer_id` ASC),
  INDEX `employee_id` (`employee_id` ASC),
  INDEX `employee_id_2` (`employee_id` ASC),
  INDEX `id` (`id` ASC),
  INDEX `id_2` (`id` ASC),
  INDEX `shipper_id` (`shipper_id` ASC),
  INDEX `shipper_id_2` (`shipper_id` ASC),
  INDEX `id_3` (`id` ASC),
  INDEX `tax_status` (`tax_status_id` ASC),
  INDEX `ship_zip_postal_code` (`ship_zip_postal_code` ASC),
  CONSTRAINT `fk_orders_customers`
    FOREIGN KEY (`customer_id`)
    REFERENCES `customers` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_orders_employees1`
    FOREIGN KEY (`employee_id`)
    REFERENCES `employees` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_orders_shippers1`
    FOREIGN KEY (`shipper_id`)
    REFERENCES `shippers` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_orders_orders_tax_status1`
    FOREIGN KEY (`tax_status_id`)
    REFERENCES `orders_tax_status` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_orders_orders_status1`
    FOREIGN KEY (`status_id`)
    REFERENCES `orders_status` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `products`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `products` (
  `supplier_ids` LONGTEXT NULL DEFAULT NULL,
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `product_code` VARCHAR(25) NULL DEFAULT NULL,
  `product_name` VARCHAR(50) NULL DEFAULT NULL,
  `description` LONGTEXT NULL DEFAULT NULL,
  `standard_cost` DECIMAL(19,4) NULL DEFAULT '0.0000',
  `list_price` DECIMAL(19,4) NOT NULL DEFAULT '0.0000',
  `reorder_level` INT(11) NULL DEFAULT NULL,
  `target_level` INT(11) NULL DEFAULT NULL,
  `quantity_per_unit` VARCHAR(50) NULL DEFAULT NULL,
  `discontinued` TINYINT(1) NOT NULL DEFAULT '0',
  `minimum_reorder_quantity` INT(11) NULL DEFAULT NULL,
  `category` VARCHAR(50) NULL DEFAULT NULL,
  `attachments` LONGBLOB NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `product_code` (`product_code` ASC))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `purchase_order_status`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `purchase_order_status` (
  `id` INT(11) NOT NULL,
  `status` VARCHAR(50) NULL DEFAULT NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `suppliers`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `suppliers` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `company` VARCHAR(50) NULL DEFAULT NULL,
  `last_name` VARCHAR(50) NULL DEFAULT NULL,
  `first_name` VARCHAR(50) NULL DEFAULT NULL,
  `email_address` VARCHAR(50) NULL DEFAULT NULL,
  `job_title` VARCHAR(50) NULL DEFAULT NULL,
  `business_phone` VARCHAR(25) NULL DEFAULT NULL,
  `home_phone` VARCHAR(25) NULL DEFAULT NULL,
  `mobile_phone` VARCHAR(25) NULL DEFAULT NULL,
  `fax_number` VARCHAR(25) NULL DEFAULT NULL,
  `address` LONGTEXT NULL DEFAULT NULL,
  `city` VARCHAR(50) NULL DEFAULT NULL,
  `state_province` VARCHAR(50) NULL DEFAULT NULL,
  `zip_postal_code` VARCHAR(15) NULL DEFAULT NULL,
  `country_region` VARCHAR(50) NULL DEFAULT NULL,
  `web_page` LONGTEXT NULL DEFAULT NULL,
  `notes` LONGTEXT NULL DEFAULT NULL,
  `attachments` LONGBLOB NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `city` (`city` ASC),
  INDEX `company` (`company` ASC),
  INDEX `first_name` (`first_name` ASC),
  INDEX `last_name` (`last_name` ASC),
  INDEX `zip_postal_code` (`zip_postal_code` ASC),
  INDEX `state_province` (`state_province` ASC))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `purchase_orders`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `purchase_orders` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `supplier_id` INT(11) NULL DEFAULT NULL,
  `created_by` INT(11) NULL DEFAULT NULL,
  `submitted_date` DATETIME NULL DEFAULT NULL,
  `creation_date` DATETIME NULL DEFAULT NULL,
  `status_id` INT(11) NULL DEFAULT '0',
  `expected_date` DATETIME NULL DEFAULT NULL,
  `shipping_fee` DECIMAL(19,4) NOT NULL DEFAULT '0.0000',
  `taxes` DECIMAL(19,4) NOT NULL DEFAULT '0.0000',
  `payment_date` DATETIME NULL DEFAULT NULL,
  `payment_amount` DECIMAL(19,4) NULL DEFAULT '0.0000',
  `payment_method` VARCHAR(50) NULL DEFAULT NULL,
  `notes` LONGTEXT NULL DEFAULT NULL,
  `approved_by` INT(11) NULL DEFAULT NULL,
  `approved_date` DATETIME NULL DEFAULT NULL,
  `submitted_by` INT(11) NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id` (`id` ASC),
  INDEX `created_by` (`created_by` ASC),
  INDEX `status_id` (`status_id` ASC),
  INDEX `id_2` (`id` ASC),
  INDEX `supplier_id` (`supplier_id` ASC),
  INDEX `supplier_id_2` (`supplier_id` ASC),
  CONSTRAINT `fk_purchase_orders_employees1`
    FOREIGN KEY (`created_by`)
    REFERENCES `employees` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_purchase_orders_purchase_order_status1`
    FOREIGN KEY (`status_id`)
    REFERENCES `purchase_order_status` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_purchase_orders_suppliers1`
    FOREIGN KEY (`supplier_id`)
    REFERENCES `suppliers` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `inventory_transactions`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `inventory_transactions` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `transaction_type` TINYINT(4) NOT NULL,
  `transaction_created_date` DATETIME NULL DEFAULT NULL,
  `transaction_modified_date` DATETIME NULL DEFAULT NULL,
  `product_id` INT(11) NOT NULL,
  `quantity` INT(11) NOT NULL,
  `purchase_order_id` INT(11) NULL DEFAULT NULL,
  `customer_order_id` INT(11) NULL DEFAULT NULL,
  `comments` VARCHAR(255) NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `customer_order_id` (`customer_order_id` ASC),
  INDEX `customer_order_id_2` (`customer_order_id` ASC),
  INDEX `product_id` (`product_id` ASC),
  INDEX `product_id_2` (`product_id` ASC),
  INDEX `purchase_order_id` (`purchase_order_id` ASC),
  INDEX `purchase_order_id_2` (`purchase_order_id` ASC),
  INDEX `transaction_type` (`transaction_type` ASC),
  CONSTRAINT `fk_inventory_transactions_orders1`
    FOREIGN KEY (`customer_order_id`)
    REFERENCES `orders` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_inventory_transactions_products1`
    FOREIGN KEY (`product_id`)
    REFERENCES `products` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_inventory_transactions_purchase_orders1`
    FOREIGN KEY (`purchase_order_id`)
    REFERENCES `purchase_orders` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_inventory_transactions_inventory_transaction_types1`
    FOREIGN KEY (`transaction_type`)
    REFERENCES `inventory_transaction_types` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `invoices`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `invoices` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `order_id` INT(11) NULL DEFAULT NULL,
  `invoice_date` DATETIME NULL DEFAULT NULL,
  `due_date` DATETIME NULL DEFAULT NULL,
  `tax` DECIMAL(19,4) NULL DEFAULT '0.0000',
  `shipping` DECIMAL(19,4) NULL DEFAULT '0.0000',
  `amount_due` DECIMAL(19,4) NULL DEFAULT '0.0000',
  PRIMARY KEY (`id`),
  INDEX `id` (`id` ASC),
  INDEX `id_2` (`id` ASC),
  INDEX `fk_invoices_orders1_idx` (`order_id` ASC),
  CONSTRAINT `fk_invoices_orders1`
    FOREIGN KEY (`order_id`)
    REFERENCES `orders` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `order_details_status`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `order_details_status` (
  `id` INT(11) NOT NULL,
  `status_name` VARCHAR(50) NOT NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `order_details`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `order_details` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `order_id` INT(11) NOT NULL,
  `product_id` INT(11) NULL DEFAULT NULL,
  `quantity` DECIMAL(18,4) NOT NULL DEFAULT '0.0000',
  `unit_price` DECIMAL(19,4) NULL DEFAULT '0.0000',
  `discount` DOUBLE NOT NULL DEFAULT '0',
  `status_id` INT(11) NULL DEFAULT NULL,
  `date_allocated` DATETIME NULL DEFAULT NULL,
  `purchase_order_id` INT(11) NULL DEFAULT NULL,
  `inventory_id` INT(11) NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `id` (`id` ASC),
  INDEX `inventory_id` (`inventory_id` ASC),
  INDEX `id_2` (`id` ASC),
  INDEX `id_3` (`id` ASC),
  INDEX `id_4` (`id` ASC),
  INDEX `product_id` (`product_id` ASC),
  INDEX `product_id_2` (`product_id` ASC),
  INDEX `purchase_order_id` (`purchase_order_id` ASC),
  INDEX `id_5` (`id` ASC),
  INDEX `fk_order_details_orders1_idx` (`order_id` ASC),
  INDEX `fk_order_details_order_details_status1_idx` (`status_id` ASC),
  CONSTRAINT `fk_order_details_orders1`
    FOREIGN KEY (`order_id`)
    REFERENCES `orders` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_order_details_products1`
    FOREIGN KEY (`product_id`)
    REFERENCES `products` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_order_details_order_details_status1`
    FOREIGN KEY (`status_id`)
    REFERENCES `order_details_status` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `purchase_order_details`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `purchase_order_details` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `purchase_order_id` INT(11) NOT NULL,
  `product_id` INT(11) NULL DEFAULT NULL,
  `quantity` DECIMAL(18,4) NOT NULL,
  `unit_cost` DECIMAL(19,4) NOT NULL,
  `date_received` DATETIME NULL DEFAULT NULL,
  `posted_to_inventory` TINYINT(1) NOT NULL DEFAULT '0',
  `inventory_id` INT(11) NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `id` (`id` ASC),
  INDEX `inventory_id` (`inventory_id` ASC),
  INDEX `inventory_id_2` (`inventory_id` ASC),
  INDEX `purchase_order_id` (`purchase_order_id` ASC),
  INDEX `product_id` (`product_id` ASC),
  INDEX `product_id_2` (`product_id` ASC),
  INDEX `purchase_order_id_2` (`purchase_order_id` ASC),
  CONSTRAINT `fk_purchase_order_details_inventory_transactions1`
    FOREIGN KEY (`inventory_id`)
    REFERENCES `inventory_transactions` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_purchase_order_details_products1`
    FOREIGN KEY (`product_id`)
    REFERENCES `products` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_purchase_order_details_purchase_orders1`
    FOREIGN KEY (`purchase_order_id`)
    REFERENCES `purchase_orders` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `sales_reports`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `sales_reports` (
  `group_by` VARCHAR(50) NOT NULL,
  `display` VARCHAR(50) NULL DEFAULT NULL,
  `title` VARCHAR(50) NULL DEFAULT NULL,
  `filter_row_source` LONGTEXT NULL DEFAULT NULL,
  `default` TINYINT(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`group_by`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `strings`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `strings` (
  `string_id` INT(11) NOT NULL AUTO_INCREMENT,
  `string_data` VARCHAR(255) NULL DEFAULT NULL,
  PRIMARY KEY (`string_id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;




-- -- Extra Tables

CREATE TABLE IF NOT EXISTS `NoPkTable` (
    `id` INT (11) NOT NULL 
) ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


CREATE VIEW  `us_customers`
AS SELECT * FROM customers
WHERE country_region = 'US';
