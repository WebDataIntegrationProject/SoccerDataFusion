package de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.Locale;

import de.uni_mannheim.informatik.dws.winter.datafusion.CorrespondenceSet;
import de.uni_mannheim.informatik.dws.winter.datafusion.DataFusionEngine;
import de.uni_mannheim.informatik.dws.winter.datafusion.DataFusionEvaluator;
import de.uni_mannheim.informatik.dws.winter.datafusion.DataFusionStrategy;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.FusibleDataSet;
import de.uni_mannheim.informatik.dws.winter.model.FusibleHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.HashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.RecordGroupFactory;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.club.CityOfStadiumEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.club.ClubNameEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.club.CountryEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.club.LeagueEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.club.NameOfStadiumEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.club.PlayersEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.club.StadiumCapacityEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.club.CityOfStadiumFuserVoting;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.club.ClubNameFuserLongestString;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.club.CountryFuserFavourSources;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.club.LeagueFuserMostRecent;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.club.NameOfStadiumFuserLongestString;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.club.PlayersFuserUnion;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.club.StadiumCapacityFuserVoting;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model.Club;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model.ClubXMLFormatter;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model.ClubXMLReader;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model.FusibleClubFactory;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model.Player;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model.PlayerXMLReader;

public class ClubApp 
{
    public static void main( String[] args ) throws Exception
    {
		// Load the Data into FusibleDataSet
//		FusibleDataSet<Club, Attribute> dbpedia = new FusibleHashedDataSet<>();
//		new ClubXMLReader().loadFromXML(new File("data/input/dbpedia.xml"), "/clubs/club", dbpedia);
//		dbpedia.printDataSetDensityReport();

//		FusibleDataSet<Club, Attribute> euro2016 = new FusibleHashedDataSet<>();
//		new ClubXMLReader().loadFromXML(new File("data/input/euro2016.xml"), "/clubs/club", euro2016);
//		euro2016.printDataSetDensityReport();
//		System.out.println(euro2016.getRandomRecord());

		FusibleDataSet<Club, Attribute> jokecamp = new FusibleHashedDataSet<>();
		new ClubXMLReader().loadFromXML(new File("data/input/jokecamp-others.xml"), "/clubs/club", jokecamp);
		jokecamp.printDataSetDensityReport();
		
		FusibleDataSet<Club, Attribute> kaggle = new FusibleHashedDataSet<>();
		new ClubXMLReader().loadFromXML(new File("data/input/kaggle.xml"), "/clubs/club", kaggle);
		kaggle.printDataSetDensityReport();
//		System.out.println(kaggle.getRandomRecord());
//		System.out.println(kaggle.getRecord("Kaggle_FC_Bayern_Munich3184"));
		
//		FusibleDataSet<Club, Attribute> transfermarket = new FusibleHashedDataSet<>();
//		new ClubXMLReader().loadFromXML(new File("data/input/transfermarket.xml"), "/clubs/club", transfermarket);
//		transfermarket.printDataSetDensityReport();
		
		// Create a HashMap from player ids to the fused player
		HashedDataSet<Player, Attribute> fusedPlayers = new HashedDataSet<>();
		new PlayerXMLReader().loadFromXML(new File("data/output/players_fused.xml"), "/players/player", fusedPlayers);
		HashMap<String, Player> fusedPlayersMap = new HashMap<String, Player>();
		for (Player p : fusedPlayers.get()) {
			for (String originalId : p.getIdentifier().split("\\+")) {
				fusedPlayersMap.put(originalId, p);
			}
		}
		System.out.println("Size of Player Map: " + fusedPlayersMap.size());
		
		// Maintain Provenance 
		//TODO: Adjust Ratings
		// Scores (e.g. from rating)
//		dbpedia.setScore(1.0);
//		euro2016.setScore(2.0);
		jokecamp.setScore(3.0);
		kaggle.setScore(2.0);
//		transfermarket.setScore(3.0);

		// Date (e.g. last update)
		DateTimeFormatter formatter = new DateTimeFormatterBuilder()
		        .appendPattern("yyyy-MM-dd")
		        .parseDefaulting(ChronoField.CLOCK_HOUR_OF_DAY, 0)
		        .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
		        .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
		        .toFormatter(Locale.ENGLISH);
		
		//TODO: Set right time!!
//		dbpedia.setDate(LocalDateTime.parse("2012-01-01", formatter));
//		euro2016.setDate(LocalDateTime.parse("2010-01-01", formatter));
		jokecamp.setDate(LocalDateTime.parse("2008-01-01", formatter));
		kaggle.setDate(LocalDateTime.parse("2010-01-01", formatter));
//		transfermarket.setDate(LocalDateTime.parse("2008-01-01", formatter));
		
		// load correspondences TODO!!! -> insert right links		
		CorrespondenceSet<Club, Attribute> correspondences = new CorrespondenceSet<>();
		// Dummy correspondense
		correspondences.loadCorrespondences(new File("data/correspondences/dummy-jokecamp_2_kaggle_correspondences_clubs.csv"), kaggle, jokecamp);

//		correspondences.loadCorrespondences(new File("data/correspondences/dbpedia_kaggle_correspondences_clubs.csv"), dbpedia, kaggle);
//		correspondences.loadCorrespondences(new File("data/correspondences/jokecamp_kaggle_clubs_correspondences_clubs.csv"), jokecamp, kaggle);
//		correspondences.loadCorrespondences(new File("data/correspondences/transfermarket_kaggle_clubs_correspondences.csv"), transfermarket, kaggle);
//		correspondences.loadCorrespondences(new File("data/correspondences/jokecamp_dbpedia_clubs_correspondences.csv"), jokecamp, dbpedia);
//		correspondences.loadCorrespondences(new File("data/correspondences/dbpedia_euro2016_clubs_correspondences.csv"),dbpedia, euro2016);
//		correspondences.loadCorrespondences(new File("data/correspondences/jokecamp_2_kaggle_correspondences.csv"), transfermarket, jokecamp);

		// write group size distribution
		correspondences.printGroupSizeDistribution();

		// define the fusion strategy
		DataFusionStrategy<Club, Attribute> strategy = new DataFusionStrategy<>(new FusibleClubFactory());
		// add attribute fusers
		strategy.addAttributeFuser(Club.NAME, new ClubNameFuserLongestString(),new ClubNameEvaluationRule());
		strategy.addAttributeFuser(Club.COUNTRY,new CountryFuserFavourSources(), new CountryEvaluationRule());
		strategy.addAttributeFuser(Club.NAMEOFSTADIUM, new NameOfStadiumFuserLongestString(),new NameOfStadiumEvaluationRule());
		strategy.addAttributeFuser(Club.CITYOFSTADIUM,new CityOfStadiumFuserVoting(),new CityOfStadiumEvaluationRule());
		strategy.addAttributeFuser(Club.STADIUMCAPACITY, new StadiumCapacityFuserVoting(), new StadiumCapacityEvaluationRule());
		strategy.addAttributeFuser(Club.LEAGUE,new LeagueFuserMostRecent(),new LeagueEvaluationRule());
		strategy.addAttributeFuser(Club.PLAYERS,new PlayersFuserUnion(fusedPlayersMap),new PlayersEvaluationRule());
	
		// create the fusion engine
		DataFusionEngine<Club, Attribute> engine = new DataFusionEngine<>(strategy);

		// print consistency report
		engine.printClusterConsistencyReport(correspondences, null);
		
		// run the fusion
		FusibleDataSet<Club, Attribute> fusedDataSet = engine.run(correspondences, null);

		// write the result
		new ClubXMLFormatter().writeXML(new File("data/output/clubs_fused.xml"), fusedDataSet);

		// load the gold standard
//		DataSet<Club, Attribute> goldStandard = new FusibleHashedDataSet<>();
//		new ClubXMLReader().loadFromXML(new File("data/goldstandard/clubs_fused.xml"), "/clubs/club", goldStandard);

		// evaluate
//		DataFusionEvaluator<Club, Attribute> evaluator = new DataFusionEvaluator<>(
//				strategy, new RecordGroupFactory<Club, Attribute>());
//		evaluator.setVerbose(true);
//		double accuracy = evaluator.evaluate(fusedDataSet, goldStandard, null);
//
//		System.out.println(String.format("Accuracy: %.2f", accuracy));
    }
}
