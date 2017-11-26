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
package de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.club;

import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model.Club;
import de.uni_mannheim.informatik.dws.winter.datafusion.AttributeValueFuser;
import de.uni_mannheim.informatik.dws.winter.datafusion.conflictresolution.Voting;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.FusedValue;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.RecordGroup;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
/**
 * {@link AttributeValueFuser} for the stadium capacity of {@link Club}s. 
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 */
public class StadiumCapacityFuserVoting extends AttributeValueFuser<String, Club, Attribute> {

	public StadiumCapacityFuserVoting() {
		super(new Voting<String, Club, Attribute>());
	}
	
	@Override
	public boolean hasValue(Club record, Correspondence<Attribute, Matchable> correspondence) {
		return record.hasValue(Club.STADIUMCAPACITY);
	}
	
	@Override
	protected String getValue(Club record, Correspondence<Attribute, Matchable> correspondence) {
		return record.getStadiumCapacity().toString();
	}

	@Override
	public void fuse(RecordGroup<Club, Attribute> group, Club fusedRecord, Processable<Correspondence<Attribute, Matchable>> schemaCorrespondences, Attribute schemaElement) {
		FusedValue<String, Club, Attribute> fused = getFusedValue(group, schemaCorrespondences, schemaElement);
		try {
			fusedRecord.setStadiumCapacity(Integer.parseInt(fused.getValue()));
		} catch(NumberFormatException e) {
			fusedRecord.setStadiumCapacity(null);
		}
		fusedRecord.setAttributeProvenance(Club.STADIUMCAPACITY, fused.getOriginalIds());
	}

}
