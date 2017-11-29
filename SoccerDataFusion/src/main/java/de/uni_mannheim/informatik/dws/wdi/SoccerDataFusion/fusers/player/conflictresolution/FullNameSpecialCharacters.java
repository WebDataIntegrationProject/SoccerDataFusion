package de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.player.conflictresolution;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.uni_mannheim.informatik.dws.winter.datafusion.conflictresolution.ConflictResolutionFunction;
import de.uni_mannheim.informatik.dws.winter.model.FusedValue;
import de.uni_mannheim.informatik.dws.winter.model.Fusible;
import de.uni_mannheim.informatik.dws.winter.model.FusibleValue;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model.Player;

public class FullNameSpecialCharacters<ValueType, RecordType extends Matchable & Fusible<SchemaElementType>, SchemaElementType extends Matchable>
		extends ConflictResolutionFunction<ValueType, RecordType, SchemaElementType> {
	
	public FullNameSpecialCharacters() {
		super();
	}
	
	@Override
	public FusedValue<ValueType, RecordType, SchemaElementType> resolveConflict(
			Collection<FusibleValue<ValueType, RecordType, SchemaElementType>> values) {

		String specialCharName = null;
	    Pattern p = Pattern.compile("[^A-Za-z0-9()]");
	     
		for (FusibleValue<ValueType, RecordType, SchemaElementType> value : values) {
			 String currentValue = (String) value.getValue();
		     Matcher m = p.matcher(currentValue);
		     boolean b = m.find();
		     if (b){
		    	 specialCharName = currentValue;
		    	 break;
		     } else if (specialCharName == null || (specialCharName.length() < currentValue.length() && !currentValue.contains(")"))) {
		    	 specialCharName = currentValue;
		     }
		}
		 
		ValueType vtName = (ValueType) specialCharName;
		FusedValue<ValueType, RecordType, SchemaElementType> fused = new FusedValue<>(vtName);

		for (FusibleValue<ValueType, RecordType, SchemaElementType> value : values) {
			fused.addOriginalRecord(value);
		}

		return fused;
	}
}
