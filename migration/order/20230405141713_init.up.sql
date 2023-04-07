create table `orders`
(
    id          int auto_increment primary key,
    customer_id int                                   not null,
    product_id  int                                   not null,
    amount      int                                   not null,
    status      varchar(50) default 'PENDING'         not null,
    created_at  timestamp   default CURRENT_TIMESTAMP not null,
    updated_at  timestamp ON UPDATE CURRENT_TIMESTAMP null
);

create table `order_outboxes`
(
    id         int auto_increment primary key,
    content    json                                not null,
    status     tinyint   default 1                 not null,
    created_at timestamp default CURRENT_TIMESTAMP not null,
    updated_at timestamp ON UPDATE CURRENT_TIMESTAMP null,
    INDEX      status_idx (status)
)