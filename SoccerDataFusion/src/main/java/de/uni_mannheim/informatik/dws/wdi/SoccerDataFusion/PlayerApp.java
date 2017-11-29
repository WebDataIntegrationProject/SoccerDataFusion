package de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Locale;

import de.uni_mannheim.informatik.dws.winter.datafusion.CorrespondenceSet;
import de.uni_mannheim.informatik.dws.winter.datafusion.DataFusionEngine;
import de.uni_mannheim.informatik.dws.winter.datafusion.DataFusionEvaluator;
import de.uni_mannheim.informatik.dws.winter.datafusion.DataFusionStrategy;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.FusibleDataSet;
import de.uni_mannheim.informatik.dws.winter.model.FusibleHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.RecordGroup;
import de.uni_mannheim.informatik.dws.winter.model.RecordGroupFactory;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.players.BirthdateEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.players.BirthplaceEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.players.CapsEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.players.ClubMembershipAsValidOfEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.players.HeightEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.players.IsInNationalTeamEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.players.NationalityEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.players.PlayerNameEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.players.PositionEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.players.PreferredFootEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.players.ShirtNumberNationalTeamEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.players.ShirtNumberOfClubEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.players.WeightEvaluationRule;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.player.BirthDateFuserFavourSources;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.player.BirthplaceFuserVoting;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.player.CapsFuserMostRecent;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.player.ClubMembershipFuserCustomMostRecent;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.player.ClubMembershipValidAsOfFuserMostRecent;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.player.HeightFuserVoting;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.player.IsInNationalTeamFuserCustomMostRecent;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.player.IsInNationalTeamFuserMostRecent;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.player.NationalityFuserMostRecent;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.player.PlayerNameFuserLongestString;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.player.PlayerNameFuserSpecialChar;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.player.PositionFuserCustomMostRecent;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.player.PositionFuserMostRecent;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.player.PreferredFootFuserVoting;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.player.ShirtNumberFuserCustomMostRecent;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.player.ShirtNumberOfClubFuserMostRecent;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.player.ShirtNumberOfNationalTeamFuserMostRecent;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.player.WeightFuserVoting;
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
//		dbpedia.printDataSetDensityReport();
//
		FusibleDataSet<Player, Attribute> euro2016 = new FusibleHashedDataSet<>();
		new PlayerXMLReader().loadFromXML(new File("data/input/euro2016.xml"), "/clubs/club/players/player", euro2016);
//		euro2016.printDataSetDensityReport();

		FusibleDataSet<Player, Attribute> jokecamp = new FusibleHashedDataSet<>();
		new PlayerXMLReader().loadFromXML(new File("data/input/jokecamp-others.xml"), "/clubs/club/players/player", jokecamp);
//		jokecamp.printDataSetDensityReport();
		
		FusibleDataSet<Player, Attribute> kaggle = new FusibleHashedDataSet<>();
		new PlayerXMLReader().loadFromXML(new File("data/input/kaggle.xml"), "/clubs/club/players/player", kaggle);
//		kaggle.printDataSetDensityReport();
		
		FusibleDataSet<Player, Attribute> transfermarket = new FusibleHashedDataSet<>();
		new PlayerXMLReader().loadFromXML(new File("data/input/transfermarket.xml"), "/clubs/club/players/player", transfermarket);
//		transfermarket.printDataSetDensityReport();

		// Maintain Provenance
		// Scores (e.g. from rating)
		dbpedia.setScore(1.0);
		euro2016.setScore(1.5);
		jokecamp.setScore(2.0);
		kaggle.setScore(2.5);
		transfermarket.setScore(3.0);

		// Date (e.g. last update)
		DateTimeFormatter formatter = new DateTimeFormatterBuilder()
		        .appendPattern("yyyy-MM-dd")
		        .parseDefaulting(ChronoField.CLOCK_HOUR_OF_DAY, 0)
		        .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
		        .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
		        .toFormatter(Locale.ENGLISH);
		
		
		//TODO: Set right time!!
		dbpedia.setDate(LocalDateTime.parse("2017-01-01", formatter));
		euro2016.setDate(LocalDateTime.parse("2016-06-10", formatter));
		jokecamp.setDate(LocalDateTime.parse("2014-11-30", formatter));
		kaggle.setDate(LocalDateTime.parse("2016-06-16", formatter));
		transfermarket.setDate(LocalDateTime.parse("2016-05-01", formatter)); // this is the crawl time stamp (jp)

		// load correspondences TODO!!! -> insert right links
		CorrespondenceSet<Player, Attribute> correspondences = new CorrespondenceSet<>();
		// Dummy correspondense
//		correspondences.loadCorrespondences(new File("data/correspondences/dummy-jokecamp_2_kaggle_correspondences_players.csv"), jokecamp, kaggle);

//		correspondences.loadCorrespondences(new File("data/correspondences/dbpedia_2_kaggle_correspondences_players.csv"), dbpedia, kaggle);
		correspondences.loadCorrespondences(new File("data/correspondences/kaggle_2_transfermarket_correspondences_players.csv"), transfermarket, kaggle);
		correspondences.loadCorrespondences(new File("data/correspondences/dbpedia_2_euro2016_correspondences_players.csv"),dbpedia, euro2016);
//		correspondences.loadCorrespondences(new File("data/correspondences/jokecamp_2_transfermarkt_correspondences_players.csv"), transfermarket, jokecamp);

		correspondences.loadCorrespondences(new File("data/correspondences/jokecamp_2_kaggle_correspondences_players.csv"), kaggle, jokecamp);
		correspondences.loadCorrespondences(new File("data/correspondences/dbpedia_2_jokecamp_correspondences_players.csv"), dbpedia, jokecamp);

		
		// get the stats before removing groups > 5
		correspondences.printGroupSizeDistribution();

		
		// Remove all groups that include multiple players of one dataset
		Collection<RecordGroup<Player, Attribute>> recordGroups = correspondences.getRecordGroups();
		Collection<RecordGroup<Player, Attribute>> recordGroupsClone = new LinkedList<>();
		recordGroupsClone.addAll(recordGroups);
		for (RecordGroup<Player, Attribute> recordGroup : recordGroupsClone) {
			LinkedList<String> provenanceList = new LinkedList<String>();
			for (Player player : recordGroup.getRecords()) {
				String provenance = player.getProvenance();
				if (provenanceList.contains(provenance)) {
					recordGroups.remove(recordGroup);
					break;
				} else {
					provenanceList.add(provenance);
				}
			}
		}
		
		// Print players of 5-player group
//		for (RecordGroup<Player, Attribute> recordGroup : recordGroups) {
//			if (recordGroup.getSize() == 5) {
//				System.out.println("group");
//				for (Player player : recordGroup.getRecords()) {
//					System.out.println(player.getIdentifier() + " " + player.getFullName());
//				}
//			}
//		}
		
		// write group size distribution
		correspondences.printGroupSizeDistribution();

		// define the fusion strategy
		DataFusionStrategy<Player, Attribute> strategy = new DataFusionStrategy<>(new FusiblePlayerFactory());
		// add attribute fusers
		//strategy.addAttributeFuser(Player.FULLNAME, new PlayerNameFuserLongestString(),new PlayerNameEvaluationRule());
		strategy.addAttributeFuser(Player.FULLNAME, new PlayerNameFuserSpecialChar(), new PlayerNameEvaluationRule());
		//Klammern + special characters
		strategy.addAttributeFuser(Player.BIRTHPLACE,new BirthplaceFuserVoting(), new BirthplaceEvaluationRule());
		strategy.addAttributeFuser(Player.BIRTHDATE, new BirthDateFuserFavourSources(),new BirthdateEvaluationRule());
		strategy.addAttributeFuser(Player.NATIONALITY,new NationalityFuserMostRecent(),new NationalityEvaluationRule());
		strategy.addAttributeFuser(Player.HEIGHT,new HeightFuserVoting(),new HeightEvaluationRule());
		//strategy.addAttributeFuser(Player.HEIGHT,new HeightFuserAverage(),new HeightEvaluationRule());
		//strategy.addAttributeFuser(Player.HEIGHT,new HeightFuserMedian(),new HeightEvaluationRule());
		strategy.addAttributeFuser(Player.WEIGHT,new WeightFuserVoting(),new WeightEvaluationRule());
		//strategy.addAttributeFuser(Player.WEIGHT,new WeightFuserAverage(),new WeightEvaluationRule());
		//strategy.addAttributeFuser(Player.WEIGHT,new WeightFuserMedian(),new WeightEvaluationRule());
		//strategy.addAttributeFuser(Player.SHIRTNUMBEROFCLUB,new ShirtNumberOfClubFuserMostRecent(),new ShirtNumberOfClubEvaluationRule());
		strategy.addAttributeFuser(Player.SHIRTNUMBEROFCLUB, new ShirtNumberFuserCustomMostRecent(), new ShirtNumberOfClubEvaluationRule());
		strategy.addAttributeFuser(Player.POSITION,new PositionFuserCustomMostRecent(),new PositionEvaluationRule());
		//strategy.addAttributeFuser(Player.POSITION,new PositionFuserMostRecent(),new PositionEvaluationRule());
		strategy.addAttributeFuser(Player.PREFERREDFOOT,new PreferredFootFuserVoting(),new PreferredFootEvaluationRule());
		strategy.addAttributeFuser(Player.CAPS,new CapsFuserMostRecent(),new CapsEvaluationRule());
		//strategy.addAttributeFuser(Player.ISINNATIONALTEAM,new IsInNationalTeamFuserMostRecent(),new IsInNationalTeamEvaluationRule());
		strategy.addAttributeFuser(Player.ISINNATIONALTEAM,new IsInNationalTeamFuserCustomMostRecent(),new IsInNationalTeamEvaluationRule());
		//strategy.addAttributeFuser(Player.CLUBMEMBERSHIPVALIDASOF,new ClubMembershipValidAsOfFuserMostRecent(),new ClubMembershipAsValidOfEvaluationRule());
		strategy.addAttributeFuser(Player.CLUBMEMBERSHIPVALIDASOF,new ClubMembershipFuserCustomMostRecent(),new ClubMembershipAsValidOfEvaluationRule());
		
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
		new PlayerXMLReader().loadFromXML(new File("data/goldstandard/players_fused_goldstandard.xml"), "/players/player", goldStandard);

		// evaluate
		DataFusionEvaluator<Player, Attribute> evaluator = new DataFusionEvaluator<>(
				strategy, new RecordGroupFactory<Player, Attribute>());
		evaluator.setVerbose(true);
		double accuracy = evaluator.evaluate(fusedDataSet, goldStandard, null);

		System.out.println(String.format("Accuracy: %.2f", accuracy));
    }
}
