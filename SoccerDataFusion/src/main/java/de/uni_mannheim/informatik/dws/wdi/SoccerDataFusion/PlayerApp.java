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
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.BirthdateEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.BirthplaceEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.CapsEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.CityOfStadiumEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.ClubMembershipAsValidOfEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.CountryEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.HeightEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.IsInNationalTeamEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.LeagueEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.ClubNameEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.NameOfStadiumEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.NationalityEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.PlayerNameEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.PlayersEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.PositionEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.PreferredFootEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.ShirtNumberNationalTeamEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.ShirtNumberOfClubEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.WeightEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.BirthDateFuserVoting;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.BirthplaceFuserVoting;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.CityOfStadiumFuserVoting;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.CountryFuserLongestString;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.LeagueFuserMostRecent;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.ClubNameFuserLongestString;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.NameOfStadiumFuserLongestString;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.NationalityFuserLongestString;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.PlayerNameFuserLongestString;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.PlayersFuserUnion;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.PositionFuserMostRecent;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.ShirtNumberOfClubFuserMostRecent;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.ShirtNumberOfNationalTeamFuserMostRecent;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.WeightFuserMostRecent;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model.Club;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model.ClubXMLFormatter;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model.ClubXMLReader;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model.FusibleClubFactory;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model.FusiblePlayerFactory;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model.Player;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model.PlayerXMLFormatter;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model.PlayerXMLReader;

public class PlayerApp 
{
    public static void main( String[] args ) throws Exception
    {
		// Load the Data into FusibleDataSet
		FusibleDataSet<Player, Attribute> dbpedia = new FusibleHashedDataSet<>();
		new PlayerXMLReader().loadFromXML(new File("data/input/dbpedia.xml"), "/clubs/club/players/player", dbpedia);
		dbpedia.printDataSetDensityReport();

		FusibleDataSet<Player, Attribute> euro2016 = new FusibleHashedDataSet<>();
		new PlayerXMLReader().loadFromXML(new File("data/input/euro2016.xml"), "/clubs/club/players/player", euro2016);
		euro2016.printDataSetDensityReport();

		FusibleDataSet<Player, Attribute> jokecamp = new FusibleHashedDataSet<>();
		new PlayerXMLReader().loadFromXML(new File("data/input/jokecamp-others.xml"), "/clubs/club/players/player", jokecamp);
		jokecamp.printDataSetDensityReport();
		
		FusibleDataSet<Player, Attribute> kaggle = new FusibleHashedDataSet<>();
		new PlayerXMLReader().loadFromXML(new File("data/input/kaggle.xml"), "/clubs/club/players/player", kaggle);
		kaggle.printDataSetDensityReport();
		
		FusibleDataSet<Player, Attribute> transfermarket = new FusibleHashedDataSet<>();
		new PlayerXMLReader().loadFromXML(new File("data/input/transfermarket.xml"), "/clubs/club/players/player", transfermarket);
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
		CorrespondenceSet<Player, Attribute> correspondences = new CorrespondenceSet<>();
		correspondences.loadCorrespondences(new File("data/correspondences/dbpedia_kaggle_players_correspondences.csv"), dbpedia, kaggle);
		correspondences.loadCorrespondences(new File("data/correspondences/jokecamp_kaggle_players_correspondences.csv"), jokecamp, kaggle);
		correspondences.loadCorrespondences(new File("data/correspondences/transfermarket_kaggle_players_correspondences.csv"), transfermarket, kaggle);
		correspondences.loadCorrespondences(new File("data/correspondences/jokecamp_dbpedia_players_correspondences.csv"), jokecamp, dbpedia);
		correspondences.loadCorrespondences(new File("data/correspondences/dbpedia_euro2016_players_correspondences.csv"),dbpedia, euro2016);
		correspondences.loadCorrespondences(new File("data/correspondences/transfermarket_jokecamp_players_correspondences.csv"), transfermarket, jokecamp);

		// write group size distribution
		correspondences.printGroupSizeDistribution();

		// define the fusion strategy
		DataFusionStrategy<Player, Attribute> strategy = new DataFusionStrategy<>(new FusiblePlayerFactory());
		// add attribute fusers
		strategy.addAttributeFuser(Player.FULLNAME, new PlayerNameFuserLongestString(),new PlayerNameEvaluationRule());
		strategy.addAttributeFuser(Player.BIRTHPLACE,new BirthplaceFuserVoting(), new BirthplaceEvaluationRule());
		strategy.addAttributeFuser(Player.BIRTHDATE, new BirthDateFuserVoting(),new BirthdateEvaluationRule());
		strategy.addAttributeFuser(Player.NATIONALITY,new NationalityFuserLongestString(),new NationalityEvaluationRule());
		strategy.addAttributeFuser(Player.HEIGHT,new HeightFuserFavourSource(),new HeightEvaluationRule());
		strategy.addAttributeFuser(Player.WEIGHT,new WeightFuserMostRecent(),new WeightEvaluationRule());
		strategy.addAttributeFuser(Player.SHIRTNUMBEROFCLUB,new ShirtNumberOfClubFuserMostRecent(),new ShirtNumberOfClubEvaluationRule());
		strategy.addAttributeFuser(Player.SHIRTNUMBEROFNATIONALTEAM,new ShirtNumberOfNationalTeamFuserMostRecent(),new ShirtNumberNationalTeamEvaluationRule());
		strategy.addAttributeFuser(Player.POSITION,new PositionFuserMostRecent(),new PositionEvaluationRule());
		strategy.addAttributeFuser(Player.PREFERREDFOOT,new PreferredFootFuserVoting(),new PreferredFootEvaluationRule());
		strategy.addAttributeFuser(Player.CAPS,new CapsFuserHigherNumber(),new CapsEvaluationRule());
		strategy.addAttributeFuser(Player.ISINNATIONALTEAM,new IsInNationalTeamFuserMostRecent(),new IsInNationalTeamEvaluationRule());
		strategy.addAttributeFuser(Player.CLUBMEMBERSHIPVALIDASOF,new ClubMembershipFuserMostRecent(),new ClubMembershipAsValidOfEvaluationRule());
		//strategy.addAttributeFuser(Player.CLUBNAME,new PlayerClubNameFuserLongestString(),new PlayerClubNameEvaluationRule());
		
		// create the fusion engine
		DataFusionEngine<Player, Attribute> engine = new DataFusionEngine<>(strategy);

		// print consistency report
		engine.printClusterConsistencyReport(correspondences, null);
		
		// run the fusion
		FusibleDataSet<Player, Attribute> fusedDataSet = engine.run(correspondences, null);

		// write the result
		new PlayerXMLFormatter().writeXML(new File("data/output/players_fused.xml"), fusedDataSet);

		// load the gold standard
		DataSet<Player, Attribute> goldStandard = new FusibleHashedDataSet<>();
		new PlayerXMLReader().loadFromXML(new File("data/goldstandard/players_fused.xml"), "/players/player", goldStandard);

		// evaluate
		DataFusionEvaluator<Player, Attribute> evaluator = new DataFusionEvaluator<>(
				strategy, new RecordGroupFactory<Player, Attribute>());
		evaluator.setVerbose(true);
		double accuracy = evaluator.evaluate(fusedDataSet, goldStandard, null);

		System.out.println(String.format("Accuracy: %.2f", accuracy));
    }
}
