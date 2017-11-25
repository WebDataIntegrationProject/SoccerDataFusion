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

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import de.uni_mannheim.informatik.dws.winter.model.io.XMLFormatter;

/**
 * {@link XMLFormatter} for {@link Player}s.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 */
public class PlayerXMLFormatter extends XMLFormatter<Player> {

	@Override
	public Element createRootElement(Document doc) {
		return doc.createElement("players");
	}

	@Override
	public Element createElementFromRecord(Player record, Document doc) {
		Element player = doc.createElement("player");

		player.appendChild(createTextElement("id", record.getIdentifier(), doc));

		player.appendChild(createTextElement("fullName", record.getFullName(), doc));
		if (record.getBirthDate() != null) {
			player.appendChild(createTextElement("birthDate", record.getBirthDate().toLocalDate().toString(), doc));
		} else {
			player.appendChild(createTextElement("birthDate", null, doc));
		}
		player.appendChild(createTextElement("birthplace", record.getBirthplace(), doc));
		player.appendChild(createTextElement("nationality", record.getNationality(), doc));
		if (record.getHeight() != null) {
			player.appendChild(createTextElement("height", record.getHeight().toString(), doc));
		} else {
			player.appendChild(createTextElement("height", null, doc));
		}
		if (record.getWeight() != null) {
			player.appendChild(createTextElement("weight", record.getWeight().toString(), doc));
		} else {
			player.appendChild(createTextElement("weight", null, doc));
		}
		player.appendChild(createTextElement("shirtNumberOfClub", record.getShirtNumberOfClub(), doc));
		player.appendChild(createTextElement("shirtNumberOfNationalTeam", record.getShirtNumberOfNationalTeam(), doc));
		player.appendChild(createTextElement("position", record.getPosition(), doc));
		player.appendChild(createTextElement("preferredFoot", record.getPreferredFoot(), doc));
		if (record.getCaps() != null) {
			player.appendChild(createTextElement("caps", record.getCaps().toString(), doc));
		} else {
			player.appendChild(createTextElement("caps", null, doc));
		}
		if (record.getIsInNationalTeam() != null) {
			player.appendChild(createTextElement("isInNationalTeam", record.getIsInNationalTeam().toString(), doc));
		} else {
			player.appendChild(createTextElement("isInNationalTeam", null, doc));
		}
		player.appendChild(createTextElement("clubMembershipValidAsOf", record.getClubMembershipValidAsOf().toLocalDate().toString(), doc));
		
		return player;
	}

}
