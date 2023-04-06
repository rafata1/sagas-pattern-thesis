create table `accounts`
(
    customer_id int unique                          not null,
    balance     int                                 not null,
    created_at  timestamp default CURRENT_TIMESTAMP not null,
    updated_at  timestamp ON UPDATE CURRENT_TIMESTAMP null
);

create table `payment_outboxes`
(
    id         int auto_increment primary key,
    content    json                                not null,
    status     tinyint   default 1                 not null,
    created_at timestamp default CURRENT_TIMESTAMP not null,
    updated_at timestamp ON UPDATE CURRENT_TIMESTAMP null,
    INDEX      status_idx (status)
)
