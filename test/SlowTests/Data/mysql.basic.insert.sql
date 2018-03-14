INSERT INTO `order` (`o_id`, `total`) VALUES
(1, '440.00');

INSERT INTO `product` (`p_id`, `name`) VALUES
(100, 'Bread'),
(101, 'Milk');

INSERT INTO `order_item` (`oi_id`, `order_id`, `product_id`, `price`) VALUES
(10, 1, 100, '110.00'),
(11, 1, 101, '330.00');
