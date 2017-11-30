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

		if (record.getFullName() != null) {
			player.appendChild(createTextElementWithProvenance("fullName",
					record.getFullName(),
					record.getMergedAttributeProvenance(Player.FULLNAME), doc));			
		} else {
//			player.appendChild(createTextElement("fullName", null, doc));
		}
		if (record.getBirthDate() != null) {
			player.appendChild(createTextElementWithProvenance("birthDate",
					record.getBirthDate().toLocalDate().toString(),
					record.getMergedAttributeProvenance(Player.BIRTHDATE), doc));
		} else {
//			player.appendChild(createTextElement("birthDate", null, doc));
		}
		if (record.getBirthplace() != null) {
			player.appendChild(createTextElementWithProvenance("birthplace",
					record.getBirthplace(),
					record.getMergedAttributeProvenance(Player.BIRTHPLACE), doc));			
		} else {
//			player.appendChild(createTextElement("birthplace", null, doc));
		}
		if (record.getNationality() != null) {
			player.appendChild(createTextElementWithProvenance("nationality",
					record.getNationality(),
					record.getMergedAttributeProvenance(Player.NATIONALITY), doc));			
		} else {
//			player.appendChild(createTextElement("nationality", null, doc));
		}
		if (record.getHeight() != null) {
			player.appendChild(createTextElementWithProvenance("height",
					record.getHeight().toString(),
					record.getMergedAttributeProvenance(Player.HEIGHT), doc));
		} else {
//			player.appendChild(createTextElement("height", null, doc));
		}
		if (record.getWeight() != null) {
			player.appendChild(createTextElementWithProvenance("weight",
					record.getWeight().toString(),
					record.getMergedAttributeProvenance(Player.WEIGHT), doc));
		} else {
//			player.appendChild(createTextElement("weight", null, doc));
		}
		if (record.getShirtNumberOfClub() != null) {
			player.appendChild(createTextElementWithProvenance("shirtNumberOfClub",
					record.getShirtNumberOfClub(),
					record.getMergedAttributeProvenance(Player.SHIRTNUMBEROFCLUB), doc));			
		} else {
//			player.appendChild(createTextElement("shirtNumberOfClub", null, doc));
		}
		if (record.getShirtNumberOfNationalTeam() != null) {
			player.appendChild(createTextElementWithProvenance("shirtNumberOfNationalTeam",
					record.getShirtNumberOfNationalTeam(),
					record.getMergedAttributeProvenance(Player.SHIRTNUMBEROFNATIONALTEAM), doc));			
		} else {
//			player.appendChild(createTextElement("shirtNumberOfNationalTeam", null, doc));
		}
		if (record.getPosition() != null) {
			player.appendChild(createTextElementWithProvenance("position",
					record.getPosition(),
					record.getMergedAttributeProvenance(Player.POSITION), doc));
		} else {
//			player.appendChild(createTextElement("position", null, doc));
		}
		if (record.getPreferredFoot() != null) {
			player.appendChild(createTextElementWithProvenance("preferredFoot",
					record.getPreferredFoot(),
					record.getMergedAttributeProvenance(Player.PREFERREDFOOT), doc));			
		} else {
//			player.appendChild(createTextElement("preferredFoot", null, doc));
		}
		if (record.getCaps() != null) {
			player.appendChild(createTextElementWithProvenance("caps",
					record.getCaps().toString(),
					record.getMergedAttributeProvenance(Player.CAPS), doc));
		} else {
//			player.appendChild(createTextElement("caps", null, doc));
		}
		if (record.getIsInNationalTeam() != null) {
			player.appendChild(createTextElementWithProvenance("isInNationalTeam",
					record.getIsInNationalTeam().toString(),
					record.getMergedAttributeProvenance(Player.ISINNATIONALTEAM), doc));
		} else {
//			player.appendChild(createTextElement("isInNationalTeam", null, doc));
		}
		if (record.getClubMembershipValidAsOf() != null) {
			player.appendChild(createTextElementWithProvenance("clubMembershipValidAsOf",
					record.getClubMembershipValidAsOf().toLocalDate().toString(),
					record.getMergedAttributeProvenance(Player.CLUBMEMBERSHIPVALIDASOF), doc));
		} else {
//			player.appendChild(createTextElement("clubMembershipValidAsOf", null, doc));
		}
		
		return player;
	}

	protected Element createTextElementWithProvenance(String name,
			String value, String provenance, Document doc) {
		Element elem = createTextElement(name, value, doc);
		elem.setAttribute("provenance", provenance);
		return elem;
	}

}
