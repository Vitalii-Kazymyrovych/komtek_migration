DROP TABLE IF EXISTS `analytics`;

CREATE TABLE `analytics` (
                             `id` int NOT NULL AUTO_INCREMENT,
                             `topic` text COLLATE utf8mb4_unicode_ci,
                             `type` text COLLATE utf8mb4_unicode_ci,
                             `plugin_name` text COLLATE utf8mb4_unicode_ci,
                             `name` text COLLATE utf8mb4_unicode_ci,
                             `created_at` timestamp(3) NOT NULL,
                             `status` text COLLATE utf8mb4_unicode_ci NOT NULL,
                             `client_id` int NOT NULL,
                             `stream` text COLLATE utf8mb4_unicode_ci,
                             `module` text COLLATE utf8mb4_unicode_ci,
                             `last_gpu_id` int DEFAULT NULL,
                             `desired_server_id` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                             `disable_balancing` bit(1) DEFAULT NULL,
                             `start_signature` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                             `allowed_server_ids` text COLLATE utf8mb4_unicode_ci,
                             `restrictions` text COLLATE utf8mb4_unicode_ci,
                             `stream_id` int DEFAULT NULL,
                             `events_holder` text COLLATE utf8mb4_unicode_ci,
                             `start_at` timestamp(3) NULL DEFAULT NULL,
                             PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `clients`;

CREATE TABLE `clients` (
                           `id` int NOT NULL AUTO_INCREMENT,
                           `client_name` varchar(250) COLLATE utf8mb4_unicode_ci NOT NULL,
                           `country` varchar(250) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
                           `city` varchar(250) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
                           `address` varchar(250) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
                           `zip_code` varchar(15) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
                           `email` varchar(250) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
                           `phone` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
                           `comment` text COLLATE utf8mb4_unicode_ci NOT NULL,
                           PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `event_manager`;

CREATE TABLE `event_manager` (
                                 `id` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
                                 `title` text COLLATE utf8mb4_unicode_ci,
                                 `description` text COLLATE utf8mb4_unicode_ci,
                                 `created_at` timestamp(3) NULL DEFAULT NULL,
                                 `nodes` text COLLATE utf8mb4_unicode_ci,
                                 `client_id` int NOT NULL,
                                 PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `face_detections`;

CREATE TABLE `face_detections` (
                                   `id` bigint NOT NULL AUTO_INCREMENT,
                                   `list_id` int DEFAULT NULL,
                                   `list_item_id` int DEFAULT NULL,
                                   `confidence` int DEFAULT NULL,
                                   `box` blob,
                                   `face_image` text COLLATE utf8mb4_unicode_ci,
                                   `frame_image` text COLLATE utf8mb4_unicode_ci,
                                   `stream_id` int NOT NULL,
                                   `va_id` int NOT NULL,
                                   `age` varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                                   `gender` int DEFAULT NULL,
                                   `race` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                                   `spoofed` bit(1) DEFAULT NULL,
                                   `mask` bit(1) DEFAULT NULL,
                                   `created_at` timestamp(3) NOT NULL,
                                   `client_id` int DEFAULT NULL,
                                   PRIMARY KEY (`id`),
                                   KEY `face_detections_created_at` (`created_at` DESC),
                                   KEY `face_detections_list_id` (`list_id`),
                                   KEY `face_detections_list_item_id` (`list_item_id`),
                                   KEY `face_detections_confidence` (`confidence`),
                                   KEY `face_detections_created_at_stream` (`created_at` DESC,`stream_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1307243 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `face_encodings`;

CREATE TABLE `face_encodings` (
                                  `id` bigint NOT NULL AUTO_INCREMENT,
                                  `face_id` bigint DEFAULT NULL,
                                  `encoding` blob,
                                  `created_at` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                                  PRIMARY KEY (`id`),
                                  KEY `face_encodings_face_id` (`face_id`),
                                  KEY `face_encodings_created_at` (`created_at` DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `face_list_items`;

CREATE TABLE `face_list_items` (
                                   `id` int NOT NULL AUTO_INCREMENT,
                                   `name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
                                   `comment` text COLLATE utf8mb4_unicode_ci,
                                   `status` int NOT NULL DEFAULT '1',
                                   `created_at` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                                   `created_by` int NOT NULL DEFAULT '0',
                                   `closed_at` timestamp(3) NULL DEFAULT NULL,
                                   `list_id` int DEFAULT NULL,
                                   `expiration_settings` text COLLATE utf8mb4_unicode_ci,
                                   `client_id` int NOT NULL,
                                   PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=89 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `face_list_items_images`;

CREATE TABLE `face_list_items_images` (
                                          `id` int NOT NULL AUTO_INCREMENT,
                                          `list_item_id` int NOT NULL,
                                          `path` text COLLATE utf8mb4_unicode_ci NOT NULL,
                                          `encoding` blob,
                                          `points` text COLLATE utf8mb4_unicode_ci,
                                          PRIMARY KEY (`id`),
                                          KEY `list_item_id` (`list_item_id`)
) ENGINE=InnoDB AUTO_INCREMENT=134 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `face_lists`;

CREATE TABLE `face_lists` (
                              `id` int NOT NULL AUTO_INCREMENT,
                              `name` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL,
                              `comment` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                              `min_confidence` int NOT NULL DEFAULT '80',
                              `streams` text COLLATE utf8mb4_unicode_ci,
                              `send_internal_notifications` bit(1) NOT NULL DEFAULT b'0',
                              `events_holder` text COLLATE utf8mb4_unicode_ci,
                              `status` int NOT NULL DEFAULT '1',
                              `created_at` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                              `client_id` int NOT NULL,
                              `color` char(7) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '#FFFFFF',
                              `time_attendance` text COLLATE utf8mb4_unicode_ci,
                              `enabled` bit(1) DEFAULT b'0',
                              `list_permissions` text COLLATE utf8mb4_unicode_ci,
                              PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `face_notifications`;

CREATE TABLE `face_notifications` (
                                      `id` bigint NOT NULL AUTO_INCREMENT,
                                      `face_id` bigint NOT NULL,
                                      `status` int NOT NULL DEFAULT '1',
                                      `accepted_by` int NOT NULL DEFAULT '0',
                                      `created_at` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                                      PRIMARY KEY (`id`),
                                      KEY `face_notifications_face_id` (`face_id`),
                                      KEY `face_notifications_row_id_status_alert_name_created_at` (`face_id`,`status`,`created_at`)
) ENGINE=InnoDB AUTO_INCREMENT=217174 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `face_unique_person_mapping`;

CREATE TABLE `face_unique_person_mapping` (
                                              `id` char(36) COLLATE utf8mb4_unicode_ci NOT NULL,
                                              `face_id` bigint NOT NULL,
                                              `list_id` int DEFAULT NULL,
                                              `created_at` timestamp(3) NOT NULL,
                                              KEY `face_unique_person_mapping_face_uuid` (`id`),
                                              KEY `face_unique_person_mapping_face_id` (`face_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `gender_age_stat`;

CREATE TABLE `gender_age_stat` (
                                   `id` int NOT NULL AUTO_INCREMENT,
                                   `va_id` int NOT NULL,
                                   `stream_id` int NOT NULL,
                                   `gender` int NOT NULL,
                                   `age` int NOT NULL,
                                   `count` int NOT NULL,
                                   `date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                   `client_id` int NOT NULL,
                                   PRIMARY KEY (`id`),
                                   KEY `date_gender_age` (`date`),
                                   KEY `date_va_id_gender_age` (`date`,`va_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `gun_notifications`;

CREATE TABLE `gun_notifications` (
                                     `id` bigint NOT NULL AUTO_INCREMENT,
                                     `stream_id` int NOT NULL,
                                     `va_id` int NOT NULL,
                                     `frame_image` text COLLATE utf8mb4_unicode_ci NOT NULL,
                                     `thumbnail_image` text COLLATE utf8mb4_unicode_ci,
                                     `objects` text COLLATE utf8mb4_unicode_ci NOT NULL,
                                     `zone` text COLLATE utf8mb4_unicode_ci NOT NULL,
                                     `created_at` timestamp(3) NOT NULL,
                                     `client_id` int NOT NULL,
                                     `status` int NOT NULL DEFAULT '1',
                                     `accepted_by` int NOT NULL DEFAULT '0',
                                     PRIMARY KEY (`id`),
                                     KEY `gun_detection_id_status_created_at` (`id`,`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `gun_type_mapping`;

CREATE TABLE `gun_type_mapping` (
                                    `id` bigint NOT NULL AUTO_INCREMENT,
                                    `notification_id` bigint NOT NULL,
                                    `type` int NOT NULL,
                                    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `hardhats_notifications`;

CREATE TABLE `hardhats_notifications` (
                                          `id` int NOT NULL AUTO_INCREMENT,
                                          `status` int NOT NULL DEFAULT '1',
                                          `accepted_by` int NOT NULL DEFAULT '0',
                                          `objects` text COLLATE utf8mb4_unicode_ci,
                                          `stream_id` int NOT NULL,
                                          `va_id` int NOT NULL,
                                          `frame_image` text COLLATE utf8mb4_unicode_ci NOT NULL,
                                          `thumbnail_image` text COLLATE utf8mb4_unicode_ci,
                                          `created_at` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                                          `zone` text COLLATE utf8mb4_unicode_ci,
                                          `client_id` int DEFAULT NULL,
                                          PRIMARY KEY (`id`),
                                          KEY `hardhats_id_status_created_at` (`id`,`status`,`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `notifications_presence_action_types`;

CREATE TABLE `notifications_presence_action_types` (
                                                       `id` int NOT NULL AUTO_INCREMENT,
                                                       `notification_id` int NOT NULL,
                                                       `action_type` text COLLATE utf8mb4_unicode_ci,
                                                       PRIMARY KEY (`id`),
                                                       KEY `FK__notifications_presence_action_types` (`notification_id`),
                                                       CONSTRAINT `FK__notifications_presence_action_types` FOREIGN KEY (`notification_id`) REFERENCES `smart_va_notifications` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `object_in_zone_notifications`;

CREATE TABLE `object_in_zone_notifications` (
                                                `id` int NOT NULL AUTO_INCREMENT,
                                                `va_id` int DEFAULT NULL,
                                                `status` int NOT NULL DEFAULT '1',
                                                `accepted_by` int NOT NULL DEFAULT '0',
                                                `stream_id` int NOT NULL,
                                                `frame_image` text COLLATE utf8mb4_unicode_ci NOT NULL,
                                                `thumbnail_image` text COLLATE utf8mb4_unicode_ci NOT NULL,
                                                `zone` text COLLATE utf8mb4_unicode_ci,
                                                `dwell_time` int NOT NULL,
                                                `trigger` int NOT NULL,
                                                `notification_type` varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'alert',
                                                `action_type` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                                                `resolution` text COLLATE utf8mb4_unicode_ci,
                                                `created_at` timestamp(3) NOT NULL,
                                                `client_id` int DEFAULT NULL,
                                                PRIMARY KEY (`id`),
                                                KEY `id_status_created_at` (`id`,`status`,`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `object_in_zone_object_type`;

CREATE TABLE `object_in_zone_object_type` (
                                              `id` int NOT NULL AUTO_INCREMENT,
                                              `object_in_zone_id` int NOT NULL DEFAULT '1',
                                              `object_type` int DEFAULT NULL,
                                              `confidence` float DEFAULT NULL,
                                              `box` blob,
                                              PRIMARY KEY (`id`),
                                              KEY `object_in_zone_object_type_object_in_zone_id_index` (`object_in_zone_id`),
                                              CONSTRAINT `FK__object_in_zone_object_type_object_in_zone_id` FOREIGN KEY (`object_in_zone_id`) REFERENCES `object_in_zone_notifications` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `roles`;

CREATE TABLE `roles` (
                         `id` int NOT NULL AUTO_INCREMENT,
                         `role_name` varchar(60) COLLATE utf8mb4_unicode_ci NOT NULL,
                         `permissions` text COLLATE utf8mb4_unicode_ci NOT NULL,
                         `client_id` int NOT NULL,
                         PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `servers`;

CREATE TABLE `servers` (
                           `id` varchar(36) COLLATE utf8mb4_unicode_ci NOT NULL,
                           `name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
                           PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `settings`;

CREATE TABLE `settings` (
                            `Variable_name` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL,
                            `Value` text COLLATE utf8mb4_unicode_ci,
                            PRIMARY KEY (`Variable_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `smoke_fire_notifications`;

CREATE TABLE `smoke_fire_notifications` (
                                            `id` bigint NOT NULL AUTO_INCREMENT,
                                            `status` int NOT NULL DEFAULT '1',
                                            `accepted_by` int NOT NULL DEFAULT '0',
                                            `objects` text COLLATE utf8mb4_unicode_ci,
                                            `stream_id` int NOT NULL,
                                            `va_id` int NOT NULL,
                                            `frame_image` text COLLATE utf8mb4_unicode_ci NOT NULL,
                                            `thumbnail_image` text COLLATE utf8mb4_unicode_ci,
                                            `zone` text COLLATE utf8mb4_unicode_ci NOT NULL,
                                            `created_at` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                                            `client_id` int DEFAULT NULL,
                                            PRIMARY KEY (`id`),
                                            KEY `smoke_fire_id_status_created_at` (`id`,`status`,`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `smoke_fire_type_mapping`;

CREATE TABLE `smoke_fire_type_mapping` (
                                           `id` bigint NOT NULL AUTO_INCREMENT,
                                           `notification_id` bigint NOT NULL,
                                           `type` int NOT NULL,
                                           PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `stats_traffic_minutely`;

CREATE TABLE `stats_traffic_minutely` (
                                          `id` bigint NOT NULL AUTO_INCREMENT,
                                          `va_id` int NOT NULL,
                                          `line` int NOT NULL,
                                          `type` int NOT NULL,
                                          `count` int NOT NULL,
                                          `direction` int NOT NULL DEFAULT '0',
                                          `created_at` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                                          `client_id` int NOT NULL,
                                          `present` int NOT NULL DEFAULT '0',
                                          PRIMARY KEY (`id`),
                                          KEY `stats_traffic_minutely_created_at_client` (`created_at`,`client_id`),
                                          KEY `stats_traffic_minutely_created_at_va_id` (`created_at`,`va_id`,`client_id`)
) ENGINE=InnoDB AUTO_INCREMENT=273123 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `stream_groups`;

CREATE TABLE `stream_groups` (
                                 `id` int NOT NULL AUTO_INCREMENT,
                                 `parent_id` int NOT NULL,
                                 `name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
                                 `client_id` int NOT NULL,
                                 PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `streams`;

CREATE TABLE `streams` (
                           `id` int NOT NULL AUTO_INCREMENT,
                           `name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
                           `path` text COLLATE utf8mb4_unicode_ci,
                           `width` int NOT NULL,
                           `height` int NOT NULL,
                           `file_name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
                           `status` tinyint NOT NULL,
                           `created_at` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                           `lat` double NOT NULL DEFAULT '0',
                           `lng` double NOT NULL DEFAULT '0',
                           `type` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'rtsp',
                           `uuid` varchar(55) COLLATE utf8mb4_unicode_ci NOT NULL,
                           `address` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
                           `params` text COLLATE utf8mb4_unicode_ci,
                           `auth` text COLLATE utf8mb4_unicode_ci,
                           `direction` int NOT NULL DEFAULT '0',
                           `client_id` int NOT NULL,
                           `codec` varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                           `timezone` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                           `duration` bigint DEFAULT NULL,
                           `restrictions` text COLLATE utf8mb4_unicode_ci,
                           `parent_id` int NOT NULL DEFAULT '0',
                           PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=27 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `traffic_stat`;

CREATE TABLE `traffic_stat` (
                                `id` bigint NOT NULL AUTO_INCREMENT,
                                `stream_id` int NOT NULL,
                                `va_id` int NOT NULL,
                                `line` int NOT NULL,
                                `type` int NOT NULL,
                                `count` int NOT NULL,
                                `direction` int NOT NULL DEFAULT '0',
                                `created_at` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                                `client_id` int NOT NULL,
                                `frame_image` text COLLATE utf8mb4_unicode_ci,
                                `object_image` text COLLATE utf8mb4_unicode_ci,
                                `notification_status` int NOT NULL DEFAULT '0',
                                `accepted_by` int NOT NULL DEFAULT '0',
                                PRIMARY KEY (`id`),
                                KEY `traffic_stat_type` (`type`),
                                KEY `va_id` (`va_id`),
                                KEY `line` (`line`),
                                KEY `traffic_stat_notification_status_index` (`notification_status`)
) ENGINE=InnoDB AUTO_INCREMENT=860708 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `users`;

CREATE TABLE `users` (
                         `id` int NOT NULL AUTO_INCREMENT,
                         `email` varchar(250) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
                         `fullname` varchar(250) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
                         `password` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
                         `last_ip` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                         `last_login` timestamp(3) NOT NULL,
                         `ip_params` text COLLATE utf8mb4_unicode_ci NOT NULL,
                         `role_id` int NOT NULL DEFAULT '0',
                         `status` int NOT NULL DEFAULT '1',
                         `created_at` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                         `settings` text COLLATE utf8mb4_unicode_ci,
                         `client_id` int NOT NULL,
                         `client_info` text COLLATE utf8mb4_unicode_ci,
                         `type` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'basic',
                         `timezone` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                         PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `zone_exit_notifications`;

CREATE TABLE `zone_exit_notifications` (
                                           `id` int NOT NULL AUTO_INCREMENT,
                                           `va_id` int NOT NULL,
                                           `stream_id` int NOT NULL,
                                           `object_id` text COLLATE utf8mb4_unicode_ci NOT NULL,
                                           `zone_id` text COLLATE utf8mb4_unicode_ci,
                                           `seconds_in_zone` int NOT NULL,
                                           `object_type` smallint DEFAULT NULL,
                                           `notification_type` varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'exit',
                                           `created_at` timestamp(3) NOT NULL,
                                           `client_id` int DEFAULT NULL,
                                           PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `zone_exit_notifications_object_type`;

CREATE TABLE `zone_exit_notifications_object_type` (
                                                       `id` int NOT NULL AUTO_INCREMENT,
                                                       `zone_exit_notifications_id` int NOT NULL DEFAULT '1',
                                                       `object_type` int DEFAULT NULL,
                                                       PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
