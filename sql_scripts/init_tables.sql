SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

CREATE TABLE `game` (
  `id` int(11) NOT NULL,
  `map_id` varchar(64) NOT NULL,
  `min_elo` int(11) NOT NULL,
  `average_elo` int(11) NOT NULL,
  `max_elo` int(11) NOT NULL,
  `time` datetime NOT NULL,
  `is_finished` tinyint(1) NOT NULL,
  `trackmaster_limit` int(11) NOT NULL,
  `rounds` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;


CREATE TABLE `map` (
  `uid` varchar(64) NOT NULL,
  `name` varchar(128) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE `migratehistory` (
  `id` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  `migrated` datetime NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

INSERT INTO `migratehistory` (`id`, `name`, `migrated`) VALUES
(1, '0001_migration_202403301513', '2024-04-04 15:43:00'),
(2, '0002_migration_202404031759', '2024-04-04 15:43:00'),
(3, '0003_migration_202404031802', '2024-04-04 15:43:00'),
(4, '0004_migration_202404031823', '2024-04-04 15:43:00'),
(5, '0005_migration_202404031827', '2024-04-04 15:43:00'),
(6, '0006_migration_202404031832', '2024-04-04 15:43:00'),
(7, '0007_migration_202404041051', '2024-04-04 15:43:00'),
(8, '0008_migration_202404041100', '2024-04-04 15:43:00'),
(9, '0009_migration_202404052129', '2024-04-05 21:28:42'),
(10, '0010_migration_202404060927', '2024-04-06 08:40:29'),
(11, '0011_migration_202404061947', '2024-04-06 18:06:32'),
(12, '0012_migration_202404062004', '2024-04-06 18:06:32'),
(13, '0013_migration_202404120844', '2024-04-12 07:24:58'),
(14, '0014_migration_202404122008', '2024-04-12 18:27:41'),
(15, '0015_migration_202404151845', '2024-04-15 20:14:37'),
(16, '0016_migration_202404191113', '2024-04-19 09:17:09'),
(17, '0017_migration_202404201600', '2024-04-20 14:42:58'),
(18, '0018_migration_202404280910', '2024-04-28 07:13:02'),
(19, '0019_migration_202405011019', '2024-05-01 08:48:06'),
(20, '0020_migration_202405011631', '2024-05-01 15:03:33'),
(21, '0021_migration_202405041124', '2024-05-04 09:29:41'),
(22, '0022_migration_202405041356', '2024-05-04 12:56:49'),
(23, '0023_migration_202405121707', '2024-05-12 15:18:56'),
(24, '0024_migration_202410082131', '2024-10-09 06:10:39');

CREATE TABLE `player` (
  `uuid` varchar(40) NOT NULL,
  `name` text NOT NULL,
  `points` int(11) NOT NULL,
  `rank` int(11) NOT NULL,
  `last_points_update` datetime NOT NULL,
  `last_match_id` int(11) DEFAULT NULL,
  `last_name_update` datetime NOT NULL,
  `zone_id` int(11) DEFAULT NULL,
  `country_id` int(11) DEFAULT NULL,
  `club_tag` varchar(64) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE `playergame` (
  `id` int(11) NOT NULL,
  `game_id` int(11) NOT NULL,
  `player_id` varchar(40) NOT NULL,
  `is_mvp` tinyint(1) NOT NULL,
  `is_win` tinyint(1) NOT NULL,
  `position` int(11) DEFAULT NULL,
  `points` int(11) DEFAULT NULL,
  `rank_after_match` int(11) DEFAULT NULL,
  `points_after_match` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE `playerseason` (
  `id` int(11) NOT NULL,
  `season_id` int(11) NOT NULL,
  `player_id` varchar(40) NOT NULL,
  `points` int(11) NOT NULL,
  `rank` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE `season` (
  `id` int(11) NOT NULL,
  `name` varchar(32) NOT NULL,
  `start_time` datetime NOT NULL,
  `end_time` datetime NOT NULL,
  `is_aggregated` tinyint(1) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE `zone` (
  `id` int(11) NOT NULL,
  `uuid` varchar(40) NOT NULL,
  `name` text NOT NULL,
  `parent_id` int(11) DEFAULT NULL,
  `country_alpha3` varchar(10) DEFAULT NULL,
  `file_name` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

ALTER TABLE `game`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `game_id` (`id`),
  ADD KEY `game_map_id` (`map_id`),
  ADD KEY `game_max_elo` (`max_elo`),
  ADD KEY `game_average_elo` (`average_elo`),
  ADD KEY `game_min_elo` (`min_elo`);

ALTER TABLE `map`
  ADD PRIMARY KEY (`uid`);

ALTER TABLE `migratehistory`
  ADD PRIMARY KEY (`id`);

ALTER TABLE `player`
  ADD PRIMARY KEY (`uuid`),
  ADD KEY `player_rank` (`rank`),
  ADD KEY `player_points` (`points`),
  ADD KEY `player_last_match_id` (`last_match_id`),
  ADD KEY `player_zone_id` (`zone_id`),
  ADD KEY `player_country_id` (`country_id`);

ALTER TABLE `playergame`
  ADD PRIMARY KEY (`id`),
  ADD KEY `playergame_game_id` (`game_id`),
  ADD KEY `playergame_player_id` (`player_id`);

ALTER TABLE `playerseason`
  ADD PRIMARY KEY (`id`),
  ADD KEY `playerseason_season_id` (`season_id`),
  ADD KEY `playerseason_player_id` (`player_id`),
  ADD KEY `playerseason_points` (`points`),
  ADD KEY `playerseason_rank` (`rank`);

ALTER TABLE `season`
  ADD PRIMARY KEY (`id`);

ALTER TABLE `zone`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `zone_uuid` (`uuid`),
  ADD KEY `zone_parent_id` (`parent_id`);

ALTER TABLE `game`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

ALTER TABLE `migratehistory`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=25;

ALTER TABLE `playergame`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

ALTER TABLE `playerseason`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

ALTER TABLE `season`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

ALTER TABLE `zone`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

ALTER TABLE `game`
  ADD CONSTRAINT `game_ibfk_1` FOREIGN KEY (`map_id`) REFERENCES `map` (`uid`);

ALTER TABLE `player`
  ADD CONSTRAINT `player_ibfk_1` FOREIGN KEY (`last_match_id`) REFERENCES `game` (`id`),
  ADD CONSTRAINT `player_ibfk_2` FOREIGN KEY (`zone_id`) REFERENCES `zone` (`id`),
  ADD CONSTRAINT `player_ibfk_3` FOREIGN KEY (`country_id`) REFERENCES `zone` (`id`);

ALTER TABLE `playergame`
  ADD CONSTRAINT `playergame_ibfk_1` FOREIGN KEY (`game_id`) REFERENCES `game` (`id`),
  ADD CONSTRAINT `playergame_ibfk_2` FOREIGN KEY (`player_id`) REFERENCES `player` (`uuid`);

ALTER TABLE `playerseason`
  ADD CONSTRAINT `playerseason_ibfk_1` FOREIGN KEY (`season_id`) REFERENCES `season` (`id`),
  ADD CONSTRAINT `playerseason_ibfk_2` FOREIGN KEY (`player_id`) REFERENCES `player` (`uuid`);

ALTER TABLE `zone`
  ADD CONSTRAINT `zone_ibfk_1` FOREIGN KEY (`parent_id`) REFERENCES `zone` (`id`);
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
