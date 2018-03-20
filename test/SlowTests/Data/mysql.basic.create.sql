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
