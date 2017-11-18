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

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import de.uni_mannheim.informatik.dws.winter.model.io.XMLFormatter;

/**
 * {@link XMLFormatter} for {@link Club}s.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 */
public class ClubXMLFormatter extends XMLFormatter<Club> {

	PlayerXMLFormatter playerFormatter = new PlayerXMLFormatter();

	@Override
	public Element createRootElement(Document doc) {
		return doc.createElement("clubs");
	}

	@Override
	public Element createElementFromRecord(Club record, Document doc) {
		Element club = doc.createElement("club");

		club.appendChild(createTextElement("id", record.getIdentifier(), doc));

		club.appendChild(createTextElementWithProvenance("name",
				record.getName(),
				record.getMergedAttributeProvenance(Club.NAME), doc));
		club.appendChild(createTextElementWithProvenance("country",
				record.getCountry(),
				record.getMergedAttributeProvenance(Club.COUNTRY), doc));
		club.appendChild(createTextElementWithProvenance("nameOfStadium", 
				record.getNameOfStadium(), 
				record.getMergedAttributeProvenance(Club.NAMEOFSTADIUM), doc));
		club.appendChild(createTextElementWithProvenance("cityOfStadium",
				record.getCityOfStadium(),
				record.getMergedAttributeProvenance(Club.CITYOFSTADIUM), doc));
		club.appendChild(createTextElementWithProvenance("league",
				record.getLeague(),
				record.getMergedAttributeProvenance(Club.LEAGUE), doc));
		
		club.appendChild(createPlayersElement(record, doc));

		return club;
	}

	protected Element createTextElementWithProvenance(String name,
			String value, String provenance, Document doc) {
		Element elem = createTextElement(name, value, doc);
		elem.setAttribute("provenance", provenance);
		return elem;
	}

	protected Element createPlayersElement(Club record, Document doc) {
		Element playerRoot = playerFormatter.createRootElement(doc);
		playerRoot.setAttribute("provenance",
				record.getMergedAttributeProvenance(Club.PLAYERS));

		for (Player a : record.getPlayers()) {
			playerRoot.appendChild(playerFormatter
					.createElementFromRecord(a, doc));
		}

		return playerRoot;
	}

}
