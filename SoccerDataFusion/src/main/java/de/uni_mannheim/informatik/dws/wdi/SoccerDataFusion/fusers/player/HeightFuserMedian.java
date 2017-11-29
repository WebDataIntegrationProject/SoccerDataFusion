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
package de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.player;

import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model.Player;
import de.uni_mannheim.informatik.dws.winter.datafusion.AttributeValueFuser;
import de.uni_mannheim.informatik.dws.winter.datafusion.conflictresolution.numeric.Median;
import de.uni_mannheim.informatik.dws.winter.datafusion.conflictresolution.meta.MostRecent;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.FusedValue;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.RecordGroup;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;

/**
 * {@link AttributeValueFuser} for the actors of {@link Player}s.
 * 
 * 
 */
public class HeightFuserMedian extends
		AttributeValueFuser<Double, Player, Attribute> {

	public HeightFuserMedian() {
		super(new Median<Player, Attribute>());
	}

	@Override
	public boolean hasValue(Player record, Correspondence<Attribute, Matchable> correspondence) {
		return record.hasValue(Player.HEIGHT);
	}

	@Override
	protected Double getValue(Player record, Correspondence<Attribute, Matchable> correspondence) {
		return record.getHeight().doubleValue();
	}

	@Override
	public void fuse(RecordGroup<Player, Attribute> group, Player fusedRecord, Processable<Correspondence<Attribute, Matchable>> schemaCorrespondences, Attribute schemaElement) {
		FusedValue<Double, Player, Attribute> fused = getFusedValue(group, schemaCorrespondences, schemaElement);
		try {
			fusedRecord.setHeight(fused.getValue().intValue());
		} catch(NumberFormatException e) {
			fusedRecord.setHeight(null);
		}
		fusedRecord
				.setAttributeProvenance(Player.HEIGHT, fused.getOriginalIds());
	}

}
