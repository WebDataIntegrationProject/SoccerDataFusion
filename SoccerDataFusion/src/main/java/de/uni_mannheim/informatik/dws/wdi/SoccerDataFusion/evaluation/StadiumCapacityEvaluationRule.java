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
package de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation;

import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model.Club;
import de.uni_mannheim.informatik.dws.winter.datafusion.EvaluationRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;

/**
 * {@link EvaluationRule} for the directors of {@link Club}s. The rule simply
 * compares the director of two {@link Club}s and returns true, in case they
 * are identical.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 */
public class StadiumCapacityEvaluationRule extends EvaluationRule<Club, Attribute> {
	// amounnt of tolerance -> if values differ within this range, they're considered equal
	protected double threshold = 0.02;

	@Override
	public boolean isEqual(Club record1, Club record2, Attribute schemaElement) {
		if(record1.getStadiumCapacity()== null && record2.getStadiumCapacity()==null)
			return true;
		else if(record1.getStadiumCapacity()== null ^ record2.getStadiumCapacity()==null)
			return false;
		else if ((record1.getStadiumCapacity() - record2.getStadiumCapacity()) > record1.getStadiumCapacity() * threshold)  
			return false;
		else 
			return true;
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.datafusion.EvaluationRule#isEqual(java.lang.Object, java.lang.Object, de.uni_mannheim.informatik.wdi.model.Correspondence)
	 */
	@Override
	public boolean isEqual(Club record1, Club record2,
			Correspondence<Attribute, Matchable> schemaCorrespondence) {
		return isEqual(record1, record2, (Attribute)null);
	}
	
}
