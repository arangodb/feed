package datagen

import (
	"math/rand"
)

var LongWordList = []string{
	"Abbott",
	"AbbVie",
	"Abiomed",
	"Accenture",
	"Activision",
	"ADM",
	"Adobe",
	"Advance",
	"Advanced",
	"Aerospace",
	"AES",
	"Aflac",
	"Agilent",
	"Air",
	"Airlines",
	"Akamai",
	"Alaska",
	"Albemarle",
	"Alexandria",
	"Align",
	"Allegion",
	"Alliance",
	"Alliant",
	"Allstate",
	"Alphabet",
	"Altria",
	"Amazon",
	"Amcor",
	"Ameren",
	"America",
	"American",
	"Ameriprise",
	"AmerisourceBergen",
	"Ametek",
	"Amgen",
	"Amphenol",
	"Analog",
	"Analytics",
	"Ansys",
	"Anthem",
	"Aon",
	"APA",
	"Apartments",
	"Apple",
	"Applied",
	"Aptiv",
	"Arista",
	"Armour",
	"Arthur",
	"Arts",
	"Associates",
	"Assurant",
	"Atmos",
	"AT&T",
	"Auto",
	"Autodesk",
	"Automatic",
	"Automation",
	"Automotive",
	"AutoZone",
	"AvalonBay",
	"Avery",
	"Baker",
	"Ball",
	"Bancorp",
	"Bancshares",
	"Bank",
	"Bath",
	"Baxter",
	"Beauty",
	"Becton",
	"Berkley",
	"Berkshire",
	"Best",
	"Beverage",
	"Biogen",
	"Biomet",
	"Bio-Rad",
	"Bio-Techne",
	"Black",
	"BlackRock",
	"Blizzard",
	"BNY",
	"Body",
	"Boeing",
	"Booking",
	"Boots",
	"BorgWarner",
	"Boston",
	"Brands",
	"Bristol",
	"Broadcom",
	"Broadridge",
	"Brown",
	"Brown–Forman",
	"Buy",
	"Cadence",
	"Caesars",
	"Campbell",
	"Capital",
	"Cardinal",
	"Caribbean",
	"CarMax",
	"Carnival",
	"Carrier",
	"Castle",
	"Catalent",
	"Caterpillar",
	"Cboe",
	"CBRE",
	"CDW",
	"Celanese",
	"Centene",
	"CenterPoint",
	"Centers",
	"Ceridian",
	"Cerner",
	"CF",
	"Charles",
	"Charter",
	"Chase",
	"Chemical",
	"Chemicals",
	"Chevron",
	"Chipotle",
	"Chubb",
	"Church",
	"Cigna",
	"Cincinnati",
	"Cintas",
	"Cisco",
	"Citigroup",
	"Citizens",
	"Citrix",
	"City",
	"Class",
	"Clorox",
	"CME",
	"CMS",
	"Co",
	"Coca-Cola",
	"Cognizant",
	"Colgate-Palmolive",
	"Comcast",
	"Comerica",
	"Communications",
	"Communities",
	"Companies",
	"Company",
	"Conagra",
	"Connectivity",
	"ConocoPhillips",
	"Consolidated",
	"Constellation",
	"Controls",
	"Cooper",
	"Coors",
	"Copart",
	"Corning",
	"Corp",
	"Corporation",
	"Corteva",
	"Costco",
	"Coterra",
	"Crown",
	"Cruise",
	"CSX",
	"Cummins",
	"CVS",
	"Danaher",
	"Darden",
	"Data",
	"DaVita",
	"Decker",
	"Deere",
	"Delta",
	"Dennison",
	"Dentsply",
	"Depot",
	"Design",
	"Devices",
	"Devon",
	"DexCom",
	"Diagnostics",
	"Diamondback",
	"Dickinson",
	"Digital",
	"Discover",
	"Discovery",
	"Dish",
	"Disney",
	"Dollar",
	"Dominion",
	"Domino's",
	"Dover",
	"Dow",
	"DTE",
	"Duke",
	"DuPont",
	"Dwight",
	"DXC",
	"Dynamics",
	"Eastman",
	"Eaton",
	"eBay",
	"Ecolab",
	"Edison",
	"Edwards",
	"Electric",
	"Electronic",
	"Eli",
	"Emerson",
	"Energy",
	"Engineering",
	"Enphase",
	"Entergy",
	"Enterprise",
	"Entertainment",
	"EOG",
	"Equifax",
	"Equinix",
	"Equities",
	"Equity",
	"Essex",
	"Estate",
	"Estée",
	"Etsy",
	"Everest",
	"Evergy",
	"Eversource",
	"Exchange",
	"Exelon",
	"Expedia",
	"Expeditors",
	"Express",
	"Extra",
	"ExxonMobil",
	"F5",
	"Facebook",
	"Fargo",
	"Fastenal",
	"Federal",
	"FedEx",
	"Fidelity",
	"Fifth",
	"Financial",
	"First",
	"FirstEnergy",
	"Fiserv",
	"Fisher",
	"Flavors",
	"Fleetcor",
	"FMC",
	"Foods",
	"Ford",
	"Fortinet",
	"Fortive",
	"Fortune",
	"Fox",
	"Fragrances",
	"Franklin",
	"Freeport-McMoRan",
	"Freight",
	"Gallagher",
	"Gamble",
	"Gaming",
	"Gap",
	"Garmin",
	"Gartner",
	"Generac",
	"General",
	"Genuine",
	"Gilead",
	"Global",
	"Globe",
	"Goldman",
	"Grainger",
	"Grill",
	"Group",
	"Grumman",
	"Half",
	"Halliburton",
	"Hanesbrands",
	"Hartford",
	"Hasbro",
	"Hathaway",
	"HCA",
	"Health",
	"Healthcare",
	"Healthpeak",
	"Heinz",
	"Henry",
	"Hershey",
	"Hess",
	"Hewlett",
	"Hilton",
	"Holdings",
	"Hologic",
	"Home",
	"Honeywell",
	"Hormel",
	"Horton",
	"Host",
	"Hotels",
	"Howmet",
	"HP",
	"Hughes",
	"Humana",
	"Hunt",
	"Huntington",
	"IBM",
	"IDEX",
	"Idexx",
	"IHS",
	"Illinois",
	"Illumina",
	"Inc",
	"Income",
	"Incyte",
	"Industries",
	"Information",
	"Ingalls",
	"Ingersoll",
	"Instruments",
	"Intel",
	"Interactive",
	"Intercontinental",
	"International",
	"Interpublic",
	"Intuit",
	"Intuitive",
	"Invesco",
	"Investment",
	"IPG",
	"IQVIA",
	"Iron",
	"Jack",
	"Jacobs",
	"James",
	"JM",
	"Johnson",
	"JPMorgan",
	"Juniper",
	"Kansas",
	"Kellogg's",
	"KeyCorp",
	"Keysight",
	"Kimberly-Clark",
	"Kimco",
	"Kinder",
	"KLA",
	"Kraft",
	"Kroger",
	"L3Harris",
	"LabCorp",
	"Laboratories",
	"Lam",
	"Lamb",
	"Las",
	"Lauder",
	"Lauren",
	"Leggett",
	"Leidos",
	"Lennar",
	"Life",
	"Lifesciences",
	"Lilly",
	"Lincoln",
	"Linde",
	"Line",
	"Lines",
	"Live",
	"LKQ",
	"Lockheed",
	"Loews",
	"Lowe's",
	"Lumen",
	"LyondellBasell",
	"Management",
	"Marathon",
	"Marietta",
	"MarketAxess",
	"Markets",
	"Markit",
	"Marriott",
	"Marsh",
	"Martin",
	"Masco",
	"Mastercard",
	"Match",
	"Materials",
	"McCormick",
	"McDonald's",
	"McKesson",
	"McLennan",
	"Medtronic",
	"Mellon",
	"Merck",
	"MetLife",
	"Mettler",
	"Mexican",
	"MGM",
	"Micro",
	"Microchip",
	"Micron",
	"Microsoft",
	"Mid-America",
	"Mills",
	"Moderna",
	"Mohawk",
	"Molson",
	"Mondelez",
	"Monolithic",
	"Monster",
	"Moody's",
	"Morgan",
	"Morris",
	"Mosaic",
	"Motorola",
	"Motors",
	"Mountain",
	"MSCI",
	"M&T",
	"Myers",
	"Nasdaq",
	"Nation",
	"National",
	"Natural",
	"NetApp",
	"Netflix",
	"Network",
	"Networks",
	"Newell",
	"Newmont",
	"News",
	"NextEra",
	"Nielsen",
	"Nike",
	"NiSource",
	"Norfolk",
	"Northern",
	"Northrop",
	"NortonLifeLock",
	"Norwegian",
	"NRG",
	"Nucor",
	"Nvidia",
	"NVR",
	"NXP",
	"Occidental",
	"of",
	"Oil",
	"Old",
	"Omnicom",
	"One",
	"Oneok",
	"Oracle",
	"O'Reilly",
	"Organon",
	"Otis",
	"Paccar",
	"Pacific",
	"Packaging",
	"Packard",
	"Paper",
	"Parcel",
	"Parker-Hannifin",
	"Parts",
	"Paychex",
	"Paycom",
	"Payments",
	"PayPal",
	"Penn",
	"Pentair",
	"People's",
	"PepsiCo",
	"PerkinElmer",
	"Petroleum",
	"Pfizer",
	"Pharmaceutical",
	"Pharmaceuticals",
	"Philip",
	"Phillips",
	"Photonics",
	"Pinnacle",
	"Pioneer",
	"Pizza",
	"Platt",
	"PNC",
	"Pool",
	"Power",
	"PPG",
	"PPL",
	"Price",
	"Principal",
	"Processing",
	"Procter",
	"Products",
	"Progressive",
	"Prologis",
	"Properties",
	"Property",
	"Prudential",
	"PTC",
	"Public",
	"PulteGroup",
	"PVH",
	"Qorvo",
	"Qualcomm",
	"Quanta",
	"Quest",
	"Ralph",
	"Rand",
	"Raymond",
	"Raytheon",
	"Re",
	"Real",
	"Realty",
	"Regency",
	"Regeneron",
	"Regions",
	"Rentals",
	"Republic",
	"Research",
	"Residential",
	"ResMed",
	"Resorts",
	"Resources",
	"Restaurants",
	"River",
	"Robert",
	"Robinson",
	"Rockwell",
	"Rollins",
	"Roper",
	"Ross",
	"Rowe",
	"Royal",
	"Sachs",
	"Salesforce",
	"Sands",
	"SBA",
	"Schein",
	"Schlumberger",
	"Schwab",
	"Sciences",
	"Scientific",
	"Seagate",
	"Sealed",
	"Security",
	"Sempra",
	"Series",
	"Service",
	"ServiceNow",
	"Services",
	"Sherwin-Williams",
	"Simon",
	"Sirona",
	"Skyworks",
	"Smith",
	"Smucker",
	"Snap-on",
	"Solutions",
	"Soup",
	"Southern",
	"Southwest",
	"S&P",
	"Space",
	"Squibb",
	"Stanley",
	"Starbucks",
	"State",
	"Steris",
	"Storage",
	"Stores",
	"Street",
	"Stryker",
	"Supply",
	"Surgical",
	"SVB",
	"Synchrony",
	"Synopsys",
	"Sysco",
	"Systems",
	"Take-Two",
	"Tapestry",
	"Target",
	"TE",
	"Technologies",
	"Technology",
	"Teledyne",
	"Teleflex",
	"Teradyne",
	"Tesla",
	"Texas",
	"Textron",
	"The",
	"Thermo",
	"Third",
	"TJX",
	"T-Mobile",
	"Toledo",
	"Tool",
	"Tower",
	"Towers",
	"Tractor",
	"Trane",
	"TransDigm",
	"Travelers",
	"Tree",
	"Trimble",
	"Truist",
	"Trust",
	"Twitter",
	"Tyler",
	"Tyson",
	"UDR",
	"Ulta",
	"Under",
	"Union",
	"United",
	"UnitedHealth",
	"Universal",
	"U.S.",
	"US",
	"Valero",
	"Vegas",
	"Ventas",
	"Verisign",
	"Verisk",
	"Verizon",
	"Vertex",
	"VF",
	"ViacomCBS",
	"Viatris",
	"Visa",
	"Vornado",
	"Vulcan",
	"Wabtec",
	"Walgreens",
	"Walmart",
	"Walt",
	"Waste",
	"Water",
	"Waters",
	"Watson",
	"WEC",
	"Wells",
	"Welltower",
	"West",
	"Western",
	"Weston",
	"WestRock",
	"Weyerhaeuser",
	"Whirlpool",
	"Williams",
	"Willis",
	"Works",
	"Worldwide",
	"Wynn",
	"Xcel",
	"Xilinx",
	"Xylem",
	"Yum!",
	"Zebra",
	"Zimmer",
	"Zions",
	"Zoetis",
}

func ExtendLongWordList(nr int) {
	source := rand.New(rand.NewSource(int64(12345) + rand.Int63()))
	for i := 0; i < nr; i++ {
		LongWordList = append(LongWordList, MakeRandomString(rand.Intn(10)+5, source))
	}
}
