CREATE TABLE `order` (
    `o_id` INT(11) NOT NULL AUTO_INCREMENT,
    `total` DECIMAL(12,2) NULL DEFAULT 0,
    PRIMARY KEY (`o_id`)
) ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;

CREATE TABLE `product` (
    `p_id` INT(11) NOT NULL AUTO_INCREMENT,
    `name` VARCHAR (255) NOT NULL,
    PRIMARY KEY (`p_id`)
) ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


CREATE TABLE `order_item` (
    `oi_id`  INT (11) NOT NULL AUTO_INCREMENT,
    `order_id`  INT NULL,
    `product_id` INT NOT NULL,
    `price`  DECIMAL (12,2) NOT NULL DEFAULT 0,
    PRIMARY KEY (`oi_id`),
    KEY `order_id` (order_id ASC)
) ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;

ALTER TABLE `order_item`
  ADD CONSTRAINT `order_fk_ref` FOREIGN KEY (`order_id`) REFERENCES `order` (`o_id`);


ALTER TABLE `order_item`
  ADD CONSTRAINT `order_fk_ref2` FOREIGN KEY (`product_id`) REFERENCES `product` (`p_id`);




CREATE TABLE `actor` (
    a_id    INT(11) NOT NULL AUTO_INCREMENT,
    name    varchar(200)   NOT NULL,
    photo   varbinary(10)  NULL,
    PRIMARY KEY (`a_id`)
) ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;

CREATE TABLE movie (
    m_id    INT(11) NOT NULL AUTO_INCREMENT,
    name  varchar(200) NOT NULL,
    `file`   varbinary(10) NULL,
    PRIMARY KEY (`m_id`)
) ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;

CREATE TABLE actor_movie (
    a_id INT NOT NULL,
    m_id INT NOT NULL,
    PRIMARY KEY (`a_id`, `m_id`)
) ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;

ALTER TABLE actor_movie 
    ADD CONSTRAINT FK_ACTOR FOREIGN KEY (a_id)
        REFERENCES `actor` (a_id);
        
ALTER TABLE actor_movie 
    ADD CONSTRAINT FK_MOVIE FOREIGN KEY (m_id)
        REFERENCES `movie` (m_id) ;


CREATE TABLE groups1 (
    g_id INT (11) NOT NULL AUTO_INCREMENT,
    name VARCHAR (20) NOT NULL,
    parent_group_id INT NULL,
    PRIMARY KEY (`g_id`)
) ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;

ALTER TABLE groups1
    ADD CONSTRAINT FK_GROUPS1 FOREIGN KEY (parent_group_id)
        REFERENCES `groups1` (g_id);


CREATE TABLE orders2 (
   o_id           int(11) NOT NULL AUTO_INCREMENT,
   customer_vatid         int(11) not null,
   primary key (`o_id`)
) ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;

CREATE TABLE customers2 (
   c_id       int(11) NOT NULL AUTO_INCREMENT,
   vatid      int(11) not null,
   primary key (c_id)
) ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;

CREATE UNIQUE INDEX C2_VAT_ID ON customers2 (vatid);

ALTER TABLE orders2 
    ADD CONSTRAINT FK_ORDERS_VAT_ID FOREIGN KEY (customer_vatid)
        REFERENCES customers2 (vatid);
