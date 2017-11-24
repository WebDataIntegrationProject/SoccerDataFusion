package de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;

import de.uni_mannheim.informatik.dws.winter.datafusion.CorrespondenceSet;
import de.uni_mannheim.informatik.dws.winter.datafusion.DataFusionEngine;
import de.uni_mannheim.informatik.dws.winter.datafusion.DataFusionEvaluator;
import de.uni_mannheim.informatik.dws.winter.datafusion.DataFusionStrategy;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.FusibleDataSet;
import de.uni_mannheim.informatik.dws.winter.model.FusibleHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.RecordGroupFactory;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.CityOfStadiumEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.CountryEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.LeagueEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.ClubNameEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.NameOfStadiumEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.PlayersEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.CityOfStadiumFuserVoting;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.CountryFuserLongestString;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.LeagueFuserMostRecent;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.ClubNameFuserLongestString;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.NameOfStadiumFuserLongestString;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.PlayersFuserUnion;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model.Club;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model.ClubXMLFormatter;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model.ClubXMLReader;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model.FusibleClubFactory;

public class ClubApp 
{
    public static void main( String[] args ) throws Exception
    {
		// Load the Data into FusibleDataSet
		FusibleDataSet<Club, Attribute> dbpedia = new FusibleHashedDataSet<>();
		new ClubXMLReader().loadFromXML(new File("data/input/dbpedia.xml"), "/clubs/club", dbpedia);
		dbpedia.printDataSetDensityReport();

		FusibleDataSet<Club, Attribute> euro2016 = new FusibleHashedDataSet<>();
		new ClubXMLReader().loadFromXML(new File("data/input/euro2016.xml"), "/clubs/club", euro2016);
		euro2016.printDataSetDensityReport();

		FusibleDataSet<Club, Attribute> jokecamp = new FusibleHashedDataSet<>();
		new ClubXMLReader().loadFromXML(new File("data/input/jokecamp-others.xml"), "/clubs/club", jokecamp);
		jokecamp.printDataSetDensityReport();
		
		FusibleDataSet<Club, Attribute> kaggle = new FusibleHashedDataSet<>();
		new ClubXMLReader().loadFromXML(new File("data/input/kaggle.xml"), "/clubs/club", kaggle);
		kaggle.printDataSetDensityReport();
		
		FusibleDataSet<Club, Attribute> transfermarket = new FusibleHashedDataSet<>();
		new ClubXMLReader().loadFromXML(new File("data/input/transfermarket.xml"), "/clubs/club", transfermarket);
		transfermarket.printDataSetDensityReport();

		// Maintain Provenance 
		//TODO: Adjust Ratings
		// Scores (e.g. from rating)
		dbpedia.setScore(1.0);
		euro2016.setScore(2.0);
		jokecamp.setScore(3.0);
		kaggle.setScore(2.0);
		transfermarket.setScore(3.0);

		// Date (e.g. last update)
		DateTimeFormatter formatter = new DateTimeFormatterBuilder()
		        .appendPattern("yyyy-MM-dd")
		        .parseDefaulting(ChronoField.CLOCK_HOUR_OF_DAY, 0)
		        .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
		        .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
		        .toFormatter(Locale.ENGLISH);
		
		//TODO: Set right time!!
		dbpedia.setDate(LocalDateTime.parse("2012-01-01", formatter));
		euro2016.setDate(LocalDateTime.parse("2010-01-01", formatter));
		jokecamp.setDate(LocalDateTime.parse("2008-01-01", formatter));
		kaggle.setDate(LocalDateTime.parse("2010-01-01", formatter));
		transfermarket.setDate(LocalDateTime.parse("2008-01-01", formatter));

		// load correspondences TODO!!! -> insert right links
		CorrespondenceSet<Club, Attribute> correspondences = new CorrespondenceSet<>();
		correspondences.loadCorrespondences(new File("data/correspondences/dbpedia_kaggle_correspondences_clubs.csv"), dbpedia, kaggle);
	//	correspondences.loadCorrespondences(new File("data/correspondences/jokecamp_kaggle_clubs_correspondences_clubs.csv"), jokecamp, kaggle);
	//	correspondences.loadCorrespondences(new File("data/correspondences/transfermarket_kaggle_clubs_correspondences.csv"), transfermarket, kaggle);
		correspondences.loadCorrespondences(new File("data/correspondences/jokecamp_dbpedia_clubs_correspondences.csv"), jokecamp, dbpedia);
		correspondences.loadCorrespondences(new File("data/correspondences/dbpedia_euro2016_clubs_correspondences.csv"),dbpedia, euro2016);
		correspondences.loadCorrespondences(new File("data/correspondences/transfermarket_jokecamp_clubs_correspondences.csv"), transfermarket, jokecamp);

		// write group size distribution
		correspondences.printGroupSizeDistribution();

		// define the fusion strategy
		DataFusionStrategy<Club, Attribute> strategy = new DataFusionStrategy<>(new FusibleClubFactory());
		// add attribute fusers
		strategy.addAttributeFuser(Club.NAME, new ClubNameFuserLongestString(),new ClubNameEvaluationRule());
		strategy.addAttributeFuser(Club.COUNTRY,new CountryFuserLongestString(), new CountryEvaluationRule());
		strategy.addAttributeFuser(Club.NAMEOFSTADIUM, new NameOfStadiumFuserLongestString(),new NameOfStadiumEvaluationRule());
		strategy.addAttributeFuser(Club.CITYOFSTADIUM,new CityOfStadiumFuserVoting(),new CityOfStadiumEvaluationRule());
		strategy.addAttributeFuser(Club.LEAGUE,new LeagueFuserMostRecent(),new LeagueEvaluationRule());
		strategy.addAttributeFuser(Club.PLAYERS,new PlayersFuserUnion(),new PlayersEvaluationRule());
	
		// create the fusion engine
		DataFusionEngine<Club, Attribute> engine = new DataFusionEngine<>(strategy);

		// print consistency report
		engine.printClusterConsistencyReport(correspondences, null);
		
		// run the fusion
		FusibleDataSet<Club, Attribute> fusedDataSet = engine.run(correspondences, null);

		// write the result
		new ClubXMLFormatter().writeXML(new File("data/output/clubs_fused.xml"), fusedDataSet);

		// load the gold standard
		DataSet<Club, Attribute> goldStandard = new FusibleHashedDataSet<>();
		new ClubXMLReader().loadFromXML(new File("data/goldstandard/clubs_fused.xml"), "/clubs/club", goldStandard);

		// evaluate
		DataFusionEvaluator<Club, Attribute> evaluator = new DataFusionEvaluator<>(
				strategy, new RecordGroupFactory<Club, Attribute>());
		evaluator.setVerbose(true);
		double accuracy = evaluator.evaluate(fusedDataSet, goldStandard, null);

		System.out.println(String.format("Accuracy: %.2f", accuracy));
    }
}
