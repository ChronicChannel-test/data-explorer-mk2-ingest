# ************************************************************
# Sequel Ace SQL dump
# Version 20099
#
# https://sequel-ace.com/
# https://github.com/Sequel-Ace/Sequel-Ace
#
# Host: 127.0.0.1 (MySQL 9.6.0)
# Database: NAEI_global
# Generation Time: 2026-04-11 11:54:57 +0000
# ************************************************************


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
SET NAMES utf8mb4;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE='NO_AUTO_VALUE_ON_ZERO', SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


# Dump of table t_Group
# ------------------------------------------------------------

DROP TABLE IF EXISTS `t_Group`;

CREATE TABLE `t_Group` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `Group_Title` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `NFRCode` text,
  `SourceName` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `ActivityName` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='NOT sorted in order';

LOCK TABLES `t_Group` WRITE;
/*!40000 ALTER TABLE `t_Group` DISABLE KEYS */;

INSERT INTO `t_Group` (`id`, `Group_Title`, `NFRCode`, `SourceName`, `ActivityName`)
VALUES
	(1,'All',NULL,NULL,NULL),
	(2,'Advanced Stove - All fuels',NULL,'Domestic Closed Stove - Advanced',NULL),
	(3,'Advanced Stove - All fuels excluding RtB',NULL,'Domestic Closed Stove - Advanced','Anthracite; Coal; Coffee Logs; Coke; Peat; Petroleum coke; SSF;  Wood - Wet'),
	(4,'Advanced Stove - All wood',NULL,'Domestic Closed Stove - Advanced','Wood - Wet; Wood - Dry; Wood - Seasoned; Wood Briquettes'),
	(5,'Advanced Stove - RtB',NULL,'Domestic Closed Stove - Advanced','Wood - Dry; Wood - Seasoned; Wood Briquettes'),
	(6,'Advanced Stove - Wood excluding RtB',NULL,'Domestic Closed Stove - Advanced','Wood - Wet'),
	(7,'Basic Stove - All fuels',NULL,'Domestic Closed Stove - Basic',NULL),
	(8,'Basic Stove - All fuels excluding RtB',NULL,'Domestic Closed Stove - Basic','Anthracite; Coal; Coffee Logs; Coke; Peat; Petroleum coke; SSF;  Wood - Wet'),
	(9,'Basic Stove - All wood',NULL,'Domestic Closed Stove - Basic','Wood - Wet; Wood - Dry; Wood - Seasoned; Wood Briquettes'),
	(10,'Basic Stove - RtB',NULL,'Domestic Closed Stove - Basic','Wood - Dry; Wood - Seasoned; Wood Briquettes'),
	(11,'Basic Stove - Wood excluding RtB',NULL,'Domestic Closed Stove - Basic','Wood - Wet'),
	(12,'Cigarette smoking',NULL,'Cigarette smoking',NULL),
	(13,'Domestic Combustion - Residential','1A4bi; 1A4bii',NULL,NULL),
	(14,'Domestic Outdoor - All',NULL,'Domestic Outdoor',NULL),
	(15,'Domestic Outdoor - Charcoal',NULL,'Domestic Outdoor','Charcoal'),
	(16,'Domestic Outdoor - Coal',NULL,'Domestic Outdoor','Coal'),
	(17,'Ecodesign Stove - All fuels',NULL,'Domestic Closed Stove - EcoDesign',NULL),
	(18,'Ecodesign Stove - All fuels excluding RtB',NULL,'Domestic Closed Stove - EcoDesign','Anthracite; Coal; Coffee Logs; Coke; Peat; Petroleum coke; SSF;  Wood - Wet'),
	(19,'Ecodesign Stove - All wood',NULL,'Domestic Closed Stove - EcoDesign','Wood - Wet; Wood - Dry; Wood - Seasoned; Wood Briquettes'),
	(20,'Ecodesign Stove - RtB',NULL,'Domestic Closed Stove - EcoDesign','Wood - Dry; Wood - Seasoned; Wood Briquettes'),
	(21,'Ecodesign Stove - Wood excluding RtB',NULL,'Domestic Closed Stove - EcoDesign','Wood - Wet'),
	(22,'Fireplace - All fuels',NULL,'Domestic Fireplace - Standard',NULL),
	(23,'Fireplace - All fuels excluding RtB',NULL,'Domestic Fireplace - Standard','Anthracite; Coal; Coffee Logs; Coke; Peat; Petroleum coke; SSF;  Wood - Wet'),
	(24,'Fireplace - All wood',NULL,'Domestic Fireplace - Standard','Wood - Wet; Wood - Dry; Wood - Seasoned; Wood Briquettes'),
	(25,'Fireplace - RtB',NULL,'Domestic Fireplace - Standard','Wood - Dry; Wood - Seasoned; Wood Briquettes'),
	(26,'Fireplace - Wood excluding RtB',NULL,'Domestic Fireplace - Standard','Wood - Wet'),
	(27,'Fireplace & All Stoves - All fuels',NULL,'Domestic Closed Stove - EcoDesign; Domestic Closed Stove - Advanced; Domestic Closed Stove - Upgraded; Domestic Closed Stove - Basic; Domestic Fireplace - Standard',NULL),
	(28,'Fireplace & All Stoves - All fuels excluding RtB',NULL,'Domestic Closed Stove - EcoDesign; Domestic Closed Stove - Advanced; Domestic Closed Stove - Upgraded; Domestic Closed Stove - Basic; Domestic Fireplace - Standard','Anthracite; Coal; Coffee Logs; Coke; Peat; Petroleum coke; SSF;  Wood - Wet'),
	(29,'Fireplace & All Stoves - All wood',NULL,'Domestic Closed Stove - EcoDesign; Domestic Closed Stove - Advanced; Domestic Closed Stove - Upgraded; Domestic Closed Stove - Basic; Domestic Fireplace - Standard','Wood - Wet; Wood - Dry; Wood - Seasoned; Wood Briquettes'),
	(30,'Fireplace & All Stoves - RtB',NULL,'Domestic Closed Stove - EcoDesign; Domestic Closed Stove - Advanced; Domestic Closed Stove - Upgraded; Domestic Closed Stove - Basic; Domestic Fireplace - Standard','Wood - Dry; Wood - Seasoned; Wood Briquettes'),
	(31,'Fireplace & All Stoves - Wood excluding RtB',NULL,'Domestic Closed Stove - EcoDesign; Domestic Closed Stove - Advanced; Domestic Closed Stove - Upgraded; Domestic Closed Stove - Basic; Domestic Fireplace - Standard','Wood - Wet'),
	(32,'Fireplace & Stoves excluding Ecodesign - All fuels',NULL,'Domestic Closed Stove - Advanced; Domestic Closed Stove - Upgraded; Domestic Closed Stove - Basic; Domestic Fireplace - Standard',NULL),
	(33,'Fireplace & Stoves excluding Ecodesign - All fuels excluding RtB',NULL,'Domestic Closed Stove - Advanced; Domestic Closed Stove - Upgraded; Domestic Closed Stove - Basic; Domestic Fireplace - Standard','Anthracite; Coal; Coffee Logs; Coke; Peat; Petroleum coke; SSF;  Wood - Wet'),
	(34,'Fireplace & Stoves excluding Ecodesign - All wood',NULL,'Domestic Closed Stove - Advanced; Domestic Closed Stove - Upgraded; Domestic Closed Stove - Basic; Domestic Fireplace - Standard','Wood - Wet; Wood - Dry; Wood - Seasoned; Wood Briquettes'),
	(35,'Fireplace & Stoves excluding Ecodesign - RtB',NULL,'Domestic Closed Stove - Advanced; Domestic Closed Stove - Upgraded; Domestic Closed Stove - Basic; Domestic Fireplace - Standard','Wood - Dry; Wood - Seasoned; Wood Briquettes'),
	(36,'Fireplace & Stoves excluding Ecodesign - Wood excluding RtB',NULL,'Domestic Closed Stove - Advanced; Domestic Closed Stove - Upgraded; Domestic Closed Stove - Basic; Domestic Fireplace - Standard','Wood - Wet'),
	(37,'Gas Boilers',NULL,'Domestic Space Heater; Domestic Water Heater','Natural gas'),
	(38,'Power Stations - All fuels',NULL,'Power stations',NULL),
	(39,'Power Stations - All fuels excluding Natural gas',NULL,'Power stations','Burning oil; Coal; Coke; Fuel oil; Gas oil; Landfill gas; Liquid bio-fuels; LPG; MSW; OPG; Orimulsion; Petroleum coke; Scrap tyres; Sewage gas; Slurry; Sour gas; Straw; Waste oils; Wood'),
	(40,'Power Stations - Natural gas',NULL,'Power stations','Natural gas'),
	(41,'Power Stations - Wood',NULL,'Power stations','Wood'),
	(42,'Road Transport - All - Exhausts Tyre & Brake Wear',NULL,'Road transport - all vehicles LPG use; Road transport - all vehicles LRP use; Road transport - buses and coaches - cold start; Road transport - buses and coaches - motorway driving; Road transport - buses and coaches - rural driving; Road transport - buses and coaches - urban driving; Road transport - cars - cold start; Road transport - cars - evaporative; Road transport - cars - motorway driving; Road transport - cars - rural driving; Road transport - cars - urban driving; Road Transport - cars Dioxins/PCP; Road transport - general; Road transport - HGV articulated - cold start; Road transport - HGV articulated - motorway driving; Road transport - HGV articulated - rural driving; Road transport - HGV articulated - urban driving; Road transport - HGV rigid - cold start; Road transport - HGV rigid - motorway driving; Road transport - HGV rigid - rural driving; Road transport - HGV rigid - urban driving; Road Transport - HGVs/buses Dioxins; Road transport - LGVs - cold start; Road transport - LGVs - evaporative; Road transport - LGVs - motorway driving; Road transport - LGVs - rural driving; Road transport - LGVs - urban driving; Road Transport - LGVs Dioxins; Road transport - mopeds (<50cc 2st) - evaporative; Road transport - mopeds (<50cc 2st) - urban driving; Road Transport - Mopeds & Motorcycles Dioxins; Road transport - motorcycle (>50cc  2st) - evaporative; Road transport - motorcycle (>50cc  2st) - urban driving; Road transport - motorcycle (>50cc  4st) - evaporative; Road transport - motorcycle (>50cc  4st) - motorway driving; Road transport - motorcycle (>50cc  4st) - rural driving; Road transport - motorcycle (>50cc  4st) - urban driving; Road transport - resuspension; Road transport - all vehicles biofuels use',NULL),
	(43,'Road Transport - Buses & Coaches Exhaust',NULL,'Road transport - buses and coaches - motorway driving; Road transport - buses and coaches - rural driving; Road transport - buses and coaches - urban driving','DERV; Lead replacement petrol; LPG; Natural gas; Petrol'),
	(44,'Road Transport - Cars Exhaust',NULL,'Road transport - cars - cold start; Road transport - cars - motorway driving; Road transport - cars - rural driving; Road transport - cars - urban driving','DERV; Lead replacement petrol; LPG; Natural gas; Petrol'),
	(45,'Road Transport - Exhausts Only',NULL,'Road transport - all vehicles LPG use; Road transport - cars - cold start; Road transport - cars - cold start; Road transport - cars - motorway driving; Road transport - cars - motorway driving; Road transport - cars - rural driving; Road transport - cars - rural driving; Road transport - cars - urban driving; Road transport - cars - urban driving; Road transport - LGVs - cold start; Road transport - LGVs - cold start; Road transport - LGVs - motorway driving; Road transport - LGVs - motorway driving; Road transport - LGVs - rural driving; Road transport - LGVs - rural driving; Road transport - LGVs - urban driving; Road transport - LGVs - urban driving; Road transport - buses and coaches - motorway driving; Road transport - buses and coaches - rural driving; Road transport - buses and coaches - urban driving; Road transport - HGV articulated - motorway driving; Road transport - HGV articulated - motorway driving; Road transport - HGV articulated - rural driving; Road transport - HGV articulated - rural driving; Road transport - HGV articulated - urban driving; Road transport - HGV articulated - urban driving; Road transport - HGV rigid - motorway driving; Road transport - HGV rigid - motorway driving; Road transport - HGV rigid - rural driving; Road transport - HGV rigid - rural driving; Road transport - HGV rigid - urban driving; Road transport - HGV rigid - urban driving; Road transport - mopeds (<50cc 2st) - urban driving; Road transport - motorcycle (>50cc  2st) - urban driving; Road transport - motorcycle (>50cc  4st) - motorway driving; Road transport - motorcycle (>50cc  4st) - rural driving; Road transport - motorcycle (>50cc  4st) - urban driving','DERV; Lead replacement petrol; LPG; Natural gas; Petrol'),
	(46,'Road Transport - HGV Exhaust',NULL,'Road transport - HGV articulated - motorway driving; Road transport - HGV articulated - rural driving; Road transport - HGV articulated - urban driving; Road transport - HGV rigid - motorway driving; Road transport - HGV rigid - rural driving; Road transport - HGV rigid - urban driving','DERV; Lead replacement petrol; LPG; Natural gas; Petrol'),
	(47,'Road Transport - LGV Exhaust',NULL,'Road transport - LGVs - cold start; Road transport - LGVs - motorway driving; Road transport - LGVs - rural driving; Road transport - LGVs - urban driving','DERV; Lead replacement petrol; LPG; Natural gas; Petrol'),
	(48,'Road Transport - Mopeds and Motorcycles Exhaust',NULL,'Road transport - mopeds (<50cc 2st) - urban driving; Road transport - motorcycle (>50cc  2st) - urban driving; Road transport - motorcycle (>50cc  4st) - motorway driving; Road transport - motorcycle (>50cc  4st) - rural driving; Road transport - motorcycle (>50cc  4st) - urban driving','DERV; Lead replacement petrol; LPG; Natural gas; Petrol'),
	(49,'Road Transport - Tyre & Brake Wear',NULL,'Road transport - all vehicles LPG use; Road transport - all vehicles LRP use; Road transport - buses and coaches - cold start; Road transport - buses and coaches - motorway driving; Road transport - buses and coaches - rural driving; Road transport - buses and coaches - urban driving; Road transport - cars - cold start; Road transport - cars - evaporative; Road transport - cars - motorway driving; Road transport - cars - rural driving; Road transport - cars - urban driving; Road Transport - cars Dioxins/PCP; Road transport - general; Road transport - HGV articulated - cold start; Road transport - HGV articulated - motorway driving; Road transport - HGV articulated - rural driving; Road transport - HGV articulated - urban driving; Road transport - HGV rigid - cold start; Road transport - HGV rigid - motorway driving; Road transport - HGV rigid - rural driving; Road transport - HGV rigid - urban driving; Road Transport - HGVs/buses Dioxins; Road transport - LGVs - cold start; Road transport - LGVs - evaporative; Road transport - LGVs - motorway driving; Road transport - LGVs - rural driving; Road transport - LGVs - urban driving; Road Transport - LGVs Dioxins; Road transport - mopeds (<50cc 2st) - evaporative; Road transport - mopeds (<50cc 2st) - urban driving; Road Transport - Mopeds & Motorcycles Dioxins; Road transport - motorcycle (>50cc  2st) - evaporative; Road transport - motorcycle (>50cc  2st) - urban driving; Road transport - motorcycle (>50cc  4st) - evaporative; Road transport - motorcycle (>50cc  4st) - motorway driving; Road transport - motorcycle (>50cc  4st) - rural driving; Road transport - motorcycle (>50cc  4st) - urban driving; Road transport - resuspension; Road transport - all vehicles biofuels use','Brake wear; Tyre wear'),
	(50,'Upgraded Stove - All fuels',NULL,'Domestic Closed Stove - Upgraded',NULL),
	(51,'Upgraded Stove - All fuels excluding RtB',NULL,'Domestic Closed Stove - Upgraded','Anthracite; Coal; Coffee Logs; Coke; Peat; Petroleum coke; SSF;  Wood - Wet'),
	(52,'Upgraded Stove - All wood',NULL,'Domestic Closed Stove - Upgraded','Wood - Wet; Wood - Dry; Wood - Seasoned; Wood Briquettes'),
	(53,'Upgraded Stove - RtB',NULL,'Domestic Closed Stove - Upgraded','Wood - Dry; Wood - Seasoned; Wood Briquettes'),
	(54,'Upgraded Stove - Wood excluding RtB',NULL,'Domestic Closed Stove - Upgraded','Wood - Wet'),
	(55,'All Stoves - All fuels',NULL,'Domestic Closed Stove - EcoDesign; Domestic Closed Stove - Advanced; Domestic Closed Stove - Upgraded; Domestic Closed Stove - Basic',NULL),
	(56,'All Stoves - All fuels excluding RtB',NULL,'Domestic Closed Stove - EcoDesign; Domestic Closed Stove - Advanced; Domestic Closed Stove - Upgraded; Domestic Closed Stove - Basic','Anthracite; Coal; Coffee Logs; Coke; Peat; Petroleum coke; SSF;  Wood - Wet'),
	(57,'All Stoves - All wood',NULL,'Domestic Closed Stove - EcoDesign; Domestic Closed Stove - Advanced; Domestic Closed Stove - Upgraded; Domestic Closed Stove - Basic','Wood - Wet; Wood - Dry; Wood - Seasoned; Wood Briquettes'),
	(58,'All Stoves - RtB',NULL,'Domestic Closed Stove - EcoDesign; Domestic Closed Stove - Advanced; Domestic Closed Stove - Upgraded; Domestic Closed Stove - Basic','Wood - Dry; Wood - Seasoned; Wood Briquettes'),
	(59,'All Stoves - Wood excluding RtB',NULL,'Domestic Closed Stove - EcoDesign; Domestic Closed Stove - Advanced; Domestic Closed Stove - Upgraded; Domestic Closed Stove - Basic','Wood - Wet'),
	(60,'Residential Outdoor - Appliance including chimineas, firepits etc',NULL,'Residential Outdoor - Appliance including chimineas, firepits etc',NULL),
	(61,'Crematoria',NULL,'Crematoria','Cremation'),
	(62,'Power Stations - All fuels excluding Wood',NULL,'Power stations','Burning oil; Coal; Coke; Fuel oil; Gas oil; Landfill gas; Liquid bio-fuels; LPG; MSW; Natural Gas; OPG; Orimulsion; Petroleum coke; Scrap tyres; Sewage gas; Slurry; Sour gas; Straw; Waste oils'),
	(63,'Residential Outdoor - All Appliances',NULL,'Residential Outdoor - All Appliances',NULL),
	(64,'Bonfire night',NULL,'Bonfire night',NULL),
	(65,'Ecodesign Stove - Dry wood',NULL,'Domestic Closed Stove - EcoDesign','Wood - Dry'),
	(66,'Fireplace - Dry wood',NULL,'Domestic Fireplace - Standard','Wood - Dry'),
	(67,'Advanced Stove - Dry wood',NULL,'Domestic Closed Stove - Advanced','Wood - Dry'),
	(68,'Basic Stove - Dry wood',NULL,'Domestic Closed Stove - Basic','Wood - Dry'),
	(69,'Upgraded Stove - Dry wood',NULL,'Domestic Closed Stove - Upgraded','Wood - Dry'),
	(70,'Residential Outdoor - Bonfires',NULL,'Residential Outdoor - Bonfires',NULL);

/*!40000 ALTER TABLE `t_Group` ENABLE KEYS */;
UNLOCK TABLES;



/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
