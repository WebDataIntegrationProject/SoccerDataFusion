/*
 * Copyright (c) 2017 Data and Web Science Group, University of Mannheim, Germany (http://dws.informatik.uni-mannheim.de/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;

import org.w3c.dom.Node;

import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.io.XMLMatchableReader;

/**
 * A {@link XMLMatchableReader} for {@link Player}s.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 */
public class PlayerXMLReader extends XMLMatchableReader<Player, Attribute> {

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.io.XMLMatchableReader#initialiseDataset(de.uni_mannheim.informatik.wdi.model.DataSet)
	 */
	@Override
	protected void initialiseDataset(DataSet<Player, Attribute> dataset) {
		super.initialiseDataset(dataset);
		
		// the schema is defined in the Movie class and not interpreted from the file, so we have to set the attributes manually
	    dataset.addAttribute(Player.FULLNAME);
	    dataset.addAttribute(Player.BIRTHPLACE);
	    dataset.addAttribute(Player.BIRTHDATE);
	    dataset.addAttribute(Player.NATIONALITY);
	    dataset.addAttribute(Player.HEIGHT);
	    dataset.addAttribute(Player.WEIGHT);	
	    dataset.addAttribute(Player.SHIRTNUMBEROFCLUB);
	    dataset.addAttribute(Player.SHIRTNUMBEROFNATIONALTEAM);
	    dataset.addAttribute(Player.POSITION);
	    dataset.addAttribute(Player.PREFERREDFOOT);
	    dataset.addAttribute(Player.CAPS);
	    dataset.addAttribute(Player.ISINNATIONALTEAM);
	    dataset.addAttribute(Player.CLUBMEMBERSHIPVALIDASOF);
	}
	
	@Override
	public Player createModelFromElement(Node node, String provenanceInfo) {
		String id = getValueFromChildElement(node, "id");

		// create the object with id and provenance information
		Player player = new Player(id, provenanceInfo);

		// fill the attributes
		player.setFullName(getValueFromChildElement(node, "fullName"));
		player.setBirthplace(getValueFromChildElement(node, "birthplace"));
		player.setNationality(getValueFromChildElement(node, "nationality"));
		player.setShirtNumberOfClub(getValueFromChildElement(node, "shirtNumberOfClub"));
		player.setShirtNumberOfNationalTeam(getValueFromChildElement(node, "shirtNumberOfNationalTeam"));
		player.setPosition(getValueFromChildElement(node, "position"));
		player.setPreferredFoot(getValueFromChildElement(node, "preferredFoot"));
		
		// convert string to integer
		try {
			Integer height = Integer.parseInt(getValueFromChildElement(node, "height"));
			player.setHeight(height);
		} catch (Exception e) {
			player.setHeight(null);
		}
		
		try {
			Integer weight = Integer.parseInt(getValueFromChildElement(node, "weight"));
			player.setWeight(weight);
		} catch (Exception e) {
			player.setWeight(null);
		}
		
		try {
			Integer caps = Integer.parseInt(getValueFromChildElement(node, "caps"));
			player.setCaps(caps);
		} catch (Exception e) {
			player.setCaps(null);
		}
		
		// convert string to boolean
		String isInNationalTeamString = getValueFromChildElement(node, "isInNationalTeam");
		if (isInNationalTeamString == "TRUE") player.setIsInNationalTeam(true);
		else if (isInNationalTeamString == "FALSE") player.setIsInNationalTeam(false);
		else player.setIsInNationalTeam(null);

		// convert the date string into a DateTime object
		try {
			String date = getValueFromChildElement(node, "birthDate");
			if (date != null) {
				DateTimeFormatter formatter = new DateTimeFormatterBuilder()
				        .appendPattern("yyyy-MM-dd")
				        .parseDefaulting(ChronoField.CLOCK_HOUR_OF_DAY, 0)
				        .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
				        .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
				        .toFormatter(Locale.ENGLISH);
				LocalDateTime dt = LocalDateTime.parse(date, formatter);
				player.setBirthday(dt);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			String date = getValueFromChildElement(node, "clubMembershipValidAsOf");
			if (date != null) {
				DateTimeFormatter formatter = new DateTimeFormatterBuilder()
				        .appendPattern("yyyy-MM-dd")
				        .parseDefaulting(ChronoField.CLOCK_HOUR_OF_DAY, 0)
				        .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
				        .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
				        .toFormatter(Locale.ENGLISH);
				LocalDateTime dt = LocalDateTime.parse(date, formatter);
				player.setClubMembershipValidAsOf(dt);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return player;
	}

}
