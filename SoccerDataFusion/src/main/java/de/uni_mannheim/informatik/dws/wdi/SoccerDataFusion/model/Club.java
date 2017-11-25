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

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model.Player;
import de.uni_mannheim.informatik.dws.winter.model.AbstractRecord;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;

/**
 * A {@link AbstractRecord} representing a movie.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 */
public class Club extends AbstractRecord<Attribute> implements Serializable {

	/*
	 * <club>
	    <!-- Most common national name. -->
	    <name>FC Bayern München</name>
	    <!-- ISO 3166 alpha 3 country code -->
	    <country>DEU</country>
	    <nameOfStadium>Allianz Arena</nameOfStadium>
	    <!-- English name of city-->
	    <cityOfStadium>Munich</cityOfStadium>
	    <!-- integer, no thousand separator-->
	    <stadiumCapacity>75000</stadiumCapacity>
	    <!-- Name of league (national name) -->
	    <league>Bundesliga</league>
	    <players>...</players>
	   </club>
	 */

	private static final long serialVersionUID = 1L;

	public Club(String identifier, String provenance) {
		super(identifier, provenance);
		players = new LinkedList<>();
	}

	private String name;
	private String country;
	private String nameOfStadium;
	private String cityOfStadium;
	private Integer stadiumCapacity;
	private String league;
	private List<Player> players;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getNameOfStadium() {
		return nameOfStadium;
	}

	public void setNameOfStadium(String nameOfStadium) {
		this.nameOfStadium = nameOfStadium;
	}

	public String getCityOfStadium() {
		return cityOfStadium;
	}

	public void setCityOfStadium(String cityOfStadium) {
		this.cityOfStadium = cityOfStadium;
	}
	
	public Integer getStadiumCapacity() {
		return stadiumCapacity;
	}

	public void setStadiumCapacity(Integer stadiumCapacity) {
		this.stadiumCapacity = stadiumCapacity;
	}

	public String getLeague() {
		return league;
	}

	public void setLeague(String leagueLabel) {
		this.league = leagueLabel;
	}

	public List<Player> getPlayers() {
		return players;
	}

	public void setPlayers(List<Player> players) {
		this.players = players;
	}


	private Map<Attribute, Collection<String>> provenance = new HashMap<>();
	private Collection<String> recordProvenance;

	public void setRecordProvenance(Collection<String> provenance) {
		recordProvenance = provenance;
	}

	public Collection<String> getRecordProvenance() {
		return recordProvenance;
	}

	public void setAttributeProvenance(Attribute attribute,
			Collection<String> provenance) {
		this.provenance.put(attribute, provenance);
	}

	public Collection<String> getAttributeProvenance(String attribute) {
		return provenance.get(attribute);
	}

	public String getMergedAttributeProvenance(Attribute attribute) {
		Collection<String> prov = provenance.get(attribute);

		if (prov != null) {
			return StringUtils.join(prov, "+");
		} else {
			return "";
		}
	}

	public static final Attribute NAME = new Attribute("Name");
	public static final Attribute COUNTRY = new Attribute("Country");
	public static final Attribute NAMEOFSTADIUM = new Attribute("NameOfStadium");
	public static final Attribute CITYOFSTADIUM = new Attribute("CityOfStadium");
	public static final Attribute LEAGUE = new Attribute("League");
	public static final Attribute PLAYERS = new Attribute("Players");
	
	
	@Override
	public boolean hasValue(Attribute attribute) {
		if(attribute==NAME)
			return getName() != null && !getName().isEmpty();
		else if(attribute==COUNTRY)
			return getCountry() != null && !getCountry().isEmpty();
		else if(attribute==NAMEOFSTADIUM)
			return getNameOfStadium() != null && !getNameOfStadium().isEmpty();
		else if(attribute==CITYOFSTADIUM)
			return getCityOfStadium() != null && !getCityOfStadium().isEmpty();
		else if(attribute==LEAGUE)
			return getLeague() != null && !getLeague().isEmpty();
		else if(attribute==PLAYERS)
			return getPlayers() != null && getPlayers().size() > 0;
		else
			return false;
	}

	@Override
	public String toString() {
		return String.format("[Club: %s / %s / %s]", getIdentifier(), getName(),
				getCountry(), getLeague());
	}

	@Override
	public int hashCode() {
		return getIdentifier().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Club){
			return this.getIdentifier().equals(((Club) obj).getIdentifier());
		}else
			return false;
	}
	
	
	
}
