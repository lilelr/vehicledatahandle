//import java.io.IOException;
//import java.text.DecimalFormat;
//import java.text.SimpleDateFormat;
//import java.util.Date;
//import java.util.HashMap;
//import java.util.Hashtable;
//import java.util.List;
//
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableMapper;
//import org.apache.hadoop.hbase.util.Bytes;
//
//import cn.com.bps.forecast.simulator.CCarGPSInfoForTrafficLihgt;
//import cn.com.bps.jni.GCJPoint;
//import cn.com.bps.jni.TransGps;
//
//import com.bps.forecast.pojo.CounterLive;
//import com.bps.forecast.pojo.JudgeInfo;
//import com.bps.forecast.pojo.LineSimple;
//import com.bps.forecast.updown.JudgeUpdown;
//import com.bps.forecast.util.Constant;
//import com.bps.forecast.util.KernelUtil;
//import com.bps.forecast.util.Log;
//
//
//public class WCMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable>{
//
//	private   DecimalFormat DF_DOUBLE = new DecimalFormat("##0.00###");
//	private SimpleDateFormat df_day = new SimpleDateFormat("yyyyMMddHHmmss");
//	private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
//
//	Hashtable<String, String> sameRouteMap = new Hashtable<String, String>();
//	private String outDateStr="20130617";
//
//	// 数组初始化容量
//	public static final int ForecastListInitialCapacity = 10000;
//
//	private CounterLive counterCdata = new CounterLive();
//
//	@Override
//	protected void setup(TableMapper<ImmutableBytesWritable, ImmutableBytesWritable>.Context context)
//			throws IOException, InterruptedException {
//		// TODO Auto-generated method stub
//		super.setup(context);
//
//		try{
//	       Constant.fs = FileSystem.get(context.getConfiguration());
//	       CatchBasicData.catchAndInitBasic(context);
//	       catchSameRouteMap();
//		}
//		catch(Exception e)
//		{
//			e.printStackTrace();
//		}
//	}
//	private void clearBasicData()
//	{
//		Constant.LineSimpleMap.clear();
//		Constant.LineSimpleMapSortByLngAsc.clear();
//		Constant.LineStationMap.clear();
//		Constant.LineLinkMap.clear();
//		Constant.LinkMap.clear();
//		Constant.LineBasicMap.clear();
//		Constant.StationBasicMap.clear();
//		Constant.OnlyUpLine.clear();
//		Constant.OnlyDownLine.clear();
//		Constant.BothLine.clear();
//		Constant.LineUseful.clear();
//		Constant.LineForecastable.clear();
//		Constant.LineCongestionMap.clear();
//	    Constant.FoldbackAreaMap.clear();
//		Constant.firstStatParkList.clear();
////		CatchBasicData.EveryLineTrafficLightOfInOutLinkID.clear();
//	}
//	@Override
//	protected void cleanup(
//			TableMapper<ImmutableBytesWritable, ImmutableBytesWritable>.Context context)
//			throws IOException, InterruptedException {
//		// TODO Auto-generated method stub
//		super.cleanup(context);
//		clearBasicData();
//	}
//
//	public void catchSameRouteMap()
//	{
//		try{
//			for (String s : Constant.LightLine) sameRouteMap.put(s, s);
//			System.out.println("---------------------" + sameRouteMap.size());
//		}
//		catch(Exception e)
//		{
//			e.printStackTrace();
//		}
//	}
//	@Override
////	protected void map(LongWritable key ,Text value, Context contexy)
////	throws IOException, InterruptedException
////	{
////
////	}
//	protected void map(ImmutableBytesWritable key, Result row, Context context)
//			throws IOException {
//
////		if (sameRouteMap.size() > 0) return;
//
//		// 开始接收数据
//		//对HBase数据进行自定义顺序
//		try{
//		String[] tempLine =new String[12];
//		tempLine[0] = Bytes.toString(row.getValue(Bytes.toBytes("F1"), Bytes.toBytes("flag")));
//		tempLine[1] = Bytes.toString(row.getValue(Bytes.toBytes("F1"), Bytes.toBytes("name")));
//		tempLine[2] = Bytes.toString(row.getValue(Bytes.toBytes("F1"), Bytes.toBytes("companyName")));
//		tempLine[3] = Bytes.toString(row.getValue(Bytes.toBytes("F1"), Bytes.toBytes("carId")));
//		tempLine[4] = "";
//		tempLine[5] = Bytes.toString(row.getValue(Bytes.toBytes("F1"), Bytes.toBytes("lineId")));
////		if(!tempLine[5].equals("5"))
////			return;
//		tempLine[6] = Bytes.toString(row.getValue(Bytes.toBytes("F1"), Bytes.toBytes("unDown")));
//		tempLine[7] = Bytes.toString(row.getValue(Bytes.toBytes("F1"), Bytes.toBytes("time")));
//		tempLine[8] = Bytes.toString(row.getValue(Bytes.toBytes("F1"), Bytes.toBytes("lat")));
//		tempLine[9] = Bytes.toString(row.getValue(Bytes.toBytes("F1"), Bytes.toBytes("lon")));
//		tempLine[10] = Bytes.toString(row.getValue(Bytes.toBytes("F1"), Bytes.toBytes("trafficId")));
//		tempLine[11] = Bytes.toString(row.getValue(Bytes.toBytes("F1"), Bytes.toBytes("distance")));
//
////		String[] tempLine = null;
//		String savegps = null;
////		Hashtable<String, String> targetLineMap = new Hashtable<String, String>();
//
////		List<File> fileListIn = new ArrayList<File>();
////		fileListIn = OperatorFile.refreshFileList(inFile);
////		if (fileListIn == null) return;
////		Collections.sort(fileListIn, CCarGPSInfoForTrafficLihgt.SortByFileNameAsc);
////		File zf = new File(inFile);
////		if (!zf.exists()) return;
////		ZipData zd = new ZipData(inFile);
//
////		Collections.sort(fileListIn, SortByFileNameAsc);
//		TransGps transGps=new TransGps();
//			try {
//				this.counterCdata.line++;
//				// 过滤GPS
//				tempLine[2]=tempLine[2].replace("YEBUS", "BUS");
//				tempLine[2]=tempLine[2].replace("BAIBUS", "BUS");
////				tempLine = StringUtil.SplitToArray(temp, ",");
//				if (!(tempLine[0].equals("215") || tempLine[0].equals("000") || tempLine[0].equals("101")|| tempLine[0].equals("212") || tempLine[0].equals("111"))) {
//					return;
//				}
//				if (tempLine.length < 12) System.out.println("segs length short:" + tempLine);
//				if (tempLine.length < 12) return;
//				String company = tempLine[2];
//				String routeName = tempLine[5];
//				String direction_0 = "0";
//				String direction_1 = "1";
//				String key_0 = company + "_" + routeName + "_" + direction_0;
//				String key_1 = company + "_" + routeName + "_" + direction_1;
//
//				if (sameRouteMap != null && sameRouteMap.size() > 0) {
//					if (!sameRouteMap.containsKey(key_0) && !sameRouteMap.containsKey(key_1)) {
//						// 不需要处理,直接跳过
//						return;
//					}
//				}
//				// 提取原始GPS信息
//				// 提取原始GPS信息
//				if(tempLine.length<12){
//					if (tempLine[0].startsWith("000")) System.out.println("tempLine len"+tempLine);
//					return;
//				}
//				if (tempLine.length > 12) {
//					savegps = tempLine[2] + "," + tempLine[3] + "," + tempLine[4] + "," + tempLine[5] + "," + tempLine[6]
//							+ "," + tempLine[7] + "," + tempLine[8] + "," + tempLine[9] + "," + tempLine[10] + ","
//							+ tempLine[11] + "," + tempLine[12];
//				} else {
//					savegps = tempLine[2] + "," + tempLine[3] + "," + tempLine[4] + "," + tempLine[5] + "," + tempLine[6]
//							+ "," + tempLine[7] + "," + tempLine[8] + "," + tempLine[9] + "," + tempLine[10] + "," + ",";
//				}
//
//				Date date = df_day.parse(tempLine[7].trim());
//				outDateStr = sdf.format(date);
//
//				String DateStr=outDateStr;
//
//				String[] segs = null;
//				try {
//					// 数据预处理，减少坐标转换量
//					segs = CCarGPSInfoForTrafficLihgt.FilterGpsLineInfo(savegps, this.counterCdata);
//					if (segs == null) {
//						if (tempLine[0].startsWith("000")) System.out.println("segs null "+tempLine.toString());
//						return;
//					}
//
//					if (tempLine.length < 18) {//交通委[16]    公交集團GPS[18]
////					    Double.parseDouble(segs[6]);
//						try
//    					{
//							Double.parseDouble(segs[6]);
//    						Double.parseDouble(segs[7]);
//    					}
//    					catch (NumberFormatException e)
//    					{
//    						if (tempLine[0].startsWith("000")) System.out.println("NumFormat"+tempLine.toString());
//    						return;
//    					}
//					    GCJPoint point = TransGps.wgs2gcj(Double.parseDouble(segs[6]), Double.parseDouble(segs[7]));
//					    if (point == null) {
//							if (tempLine[0].startsWith("000")) System.out.println("point"+segs[6]+"    "+segs[7]);
//							return;
//						}
//					    segs[8] = ""+point.getLongitude();
//                        segs[9] = ""+point.getLatitude();
//					}
//					else
//					{
//    					segs[8] = tempLine[15];//公交集團[17]  交通[15]
//    					segs[9] = tempLine[16];//公交集團[18]  交通[16]
//    					try
//    					{
//    						Double.parseDouble(segs[8]);
//    						Double.parseDouble(segs[9]);
//    					}
//    					catch (NumberFormatException e)
//    					{
//    						if (tempLine[0].startsWith("000")) System.out.println("NumFormat"+tempLine);
//    						return;
//    					}
//					}
//
//					String keythisCar = null;
//					int simpleCurSeq = -1;
//					List<LineSimple> lineSimpleList = null;
//					LineSimple simpleCur = null;// 当前匹配的采样点
//					String[] segsOut = new String[10];
//					StringBuilder sb = new StringBuilder();
//
//
//					keythisCar = segs[0] + "_" + segs[3] + "_" +0;
//			            if (KernelUtil.GetIfGpsInRange(segs, Constant.LineCongestionMap.get(keythisCar))) {
//			                // 如果长期不变
//			            	System.out.println("gps长期不变： "+segs);
//			                return;
//			            }
//			            keythisCar = segs[0] + "_" + segs[3] + "_" +1;
//			            if (KernelUtil.GetIfGpsInRange(segs, Constant.LineCongestionMap.get(keythisCar))) {
//			                // 如果长期不变
//			            	System.out.println("gps长期不变： "+segs);
//			            	return;
//			            }
//			            // 公司_线路_车辆号,获取本辆车的信息
//			            JudgeInfo ji = KernelUtil.GetJudgeInfo(segs[0] + "_" + segs[3] + "_" + segs[1]);
//
//			            try {
//			                // 上下行判断
//			                segs = JudgeUpdown.ExecJudge(savegps, segs);
//
//			                if (segs == null) {
//			                    return;
//			                }
//			               // 已过末站，直接去除，segs[12]为当前GPS的下一站
//			               int nextStationSeq = Integer.parseInt(segs[12]);
//			                // 在初始化文件中，规定当采样点位于末站后，则下一站定为-1。
//			                // 判断是否过末站,<1表示过末站 对计数器进行重置
//			                if (nextStationSeq < 1) {
//			                    if(ji.getCurDirection()==0&&ji.getUpdownCounter()==7)
//			                    {
//			                        ji.setUpdownCounter(2);
//			                    }
//			                    if(ji.getCurDirection()==1&&ji.getUpdownCounter()==1)
//			                    {
//			                        ji.setUpdownCounter(6);
//			                    }
//			                }
//			                // 状态过滤，是否丢弃该记录
//			                if (this.isDiscard(segs)) {
//			                    return;
//			                }
//			                // 公司_线路_上下行
//			                keythisCar = segs[0] + "_" + segs[3] + "_" + segs[4];
//			                // 获取一线路的采样点列表
//			                lineSimpleList = Constant.LineSimpleMap.get(keythisCar);
//
//			                // 准备必要的信息
//			                // segs[14]表示匹配到的采样点序号
//			                simpleCurSeq = Integer.parseInt(segs[14]);
//			                // 获取当前匹配到的采样点,列表从0开始，采样点从1开始存储,因此要“-1”
//			                simpleCur = lineSimpleList.get(simpleCurSeq - 1);
//
//							// 如果位于红绿灯后100米M内
//							if (simpleCur.getDisToPrevTrafficLight()<=100) {
//								segsOut[0] = segs[3];// 线路名称
//								segsOut[1] = segs[4]; //上下行
//								segsOut[2] =  segs[1];// 车辆id
//								segsOut[3]= DF_DOUBLE.format(Double.parseDouble(segs[8]) / 3600 / 1024);//经度
//								segsOut[4] = DF_DOUBLE.format(Double.parseDouble(segs[9]) / 3600 / 1024) ;//纬度
//			                    segsOut[5] = segs[5];// 时间戳
//			                    segsOut[6] = "E";// 位于灯后
//			                    if(segs[4].equals("0"))
//			                        segsOut[7] = DF_DOUBLE.format(simpleCur.getDisToPrevTrafficLight()-ji.getDisToUpLineSimple());//灯后距离
//			                    else
//			                        segsOut[7] = DF_DOUBLE.format(simpleCur.getDisToPrevTrafficLight()-ji.getDisToDownLineSimple());//灯后距离
//			                    segsOut[8] = ""+simpleCur.getNextStation();//站序
//			                    segsOut[9] =  DF_DOUBLE.format(simpleCur.getDisToNextStation()) ;//到站距离
//								HashMap  SingleLineTrafficLightOfInOutLinkID=(HashMap)CatchBasicData.EveryLineTrafficLightOfInOutLinkID.get(keythisCar);
//								if(SingleLineTrafficLightOfInOutLinkID == null)
//								{
//									System.out.println(keythisCar);
//									System.out.println("\n\n");
//								}
//								String trafficLightId=simpleCur.getPrevTrafficLightId();
//								if(!trafficLightId.equals("0"))
//								{
//									//////////////TODO//////////////////
//									if(!SingleLineTrafficLightOfInOutLinkID.containsKey(trafficLightId))//
//										return;//
//									String InOutLinkStr=(String)SingleLineTrafficLightOfInOutLinkID.get(trafficLightId);
//									String LineTrafficDataKey=trafficLightId+"_"+InOutLinkStr;
//
//									sb.delete(0, sb.length());
//									for (String seg : segsOut) {
//										sb.append(seg + ",");
//									}
//									// 删除最后的空格
//									sb.deleteCharAt(sb.length() - 1);
//									// 加入set中，以去除重复记录
//									context.write(new ImmutableBytesWritable(Bytes.toBytes(DateStr+"#"+LineTrafficDataKey)), new ImmutableBytesWritable(Bytes.toBytes(sb.toString())));
//								}
//							}
//							//距下一个红绿灯距离小于300，距上一站大于50米
//								if(simpleCur.getDisToNextTrafficLight()<=200&&simpleCur.getDisToPrevStation()>50)
//								{
//									segsOut[0] = segs[3];// 线路名称
//									segsOut[1] = segs[4]; //上下行
//									segsOut[2] =  segs[1];// 车辆id
//									segsOut[3]= DF_DOUBLE.format(Double.parseDouble(segs[8]) / 3600 / 1024);//经度
//									segsOut[4] = DF_DOUBLE.format(Double.parseDouble(segs[9]) / 3600 / 1024) ;//纬度
//									segsOut[5] = segs[5];// 时间戳
//									segsOut[6] = "S";// 位于灯前
//									if(segs[4].equals("0"))
//									    segsOut[7] = DF_DOUBLE.format(simpleCur.getDisToNextTrafficLight()+ji.getDisToUpLineSimple());//灯后距离
//									else
//									    segsOut[7] = DF_DOUBLE.format(simpleCur.getDisToNextTrafficLight()+ji.getDisToDownLineSimple());//灯后距离
//				                    segsOut[8] = ""+simpleCur.getNextStation();//站序
//				                    segsOut[9] =  DF_DOUBLE.format(simpleCur.getDisToNextStation()) ;//到站距离
//									HashMap  SingleLineTrafficLightOfInOutLinkID=(HashMap)CatchBasicData.EveryLineTrafficLightOfInOutLinkID.get(keythisCar);
//									String trafficLightId=simpleCur.getNextTrafficLightId();
//									if(!trafficLightId.equals("0"))
//									{
//										//////////////TODO//////////////////
//										if(!SingleLineTrafficLightOfInOutLinkID.containsKey(trafficLightId))//
//											return;//
//										String InOutLinkStr=(String)SingleLineTrafficLightOfInOutLinkID.get(trafficLightId);
//										String LineTrafficDataKey=trafficLightId+"_"+InOutLinkStr;
//										sb.delete(0, sb.length());
//										for (String seg : segsOut) {
//											sb.append(seg + ",");
//										}
//										// 删除最后的空格
//										sb.deleteCharAt(sb.length() - 1);
//										// 加入set中，以去除重复记录
////										context.write(new Text(DateStr+"#"+LineTrafficDataKey), new Text(sb.toString()));
//										context.write(new ImmutableBytesWritable(Bytes.toBytes(DateStr+"#"+LineTrafficDataKey)), new ImmutableBytesWritable(Bytes.toBytes(sb.toString())));
//
//									}
//								}
//							} catch (Exception e) {
//								System.out.println("Excepion");
//								e.printStackTrace();
//						}
//							// 保证GC尽快回收内容
//
//					//Thread.sleep(1000);
//				} catch (Exception e) {
//					Log.Error(e);
//				}
//			} catch (Exception ex) {
//				ex.printStackTrace();
//			}finally
//			{
//
//			}
//		}
//		catch(Exception e)
//		{
//			e.printStackTrace();
//		}
//	}
//	// 状态处理
//
//	private boolean isDiscard(String[] segs) {
//		/*
//		 * 当判断多次上下行为一致时，数据正确性就会更高 如果系统启动时，车辆就一直偏移线路，那就一直接没有该车辆的数据返回
//		 */
//		JudgeInfo ji = KernelUtil.GetJudgeInfo(segs[0] + "_" + segs[3] + "_" + segs[1]);
//
//		// 比较当前时段获取GPS数量
//		// 当判断多次上下行为一致时，小于规定值说明没有达到初始启动最小值
//		if (ji.getGpsCounter() < Constant.GPS_COUNTER_MIN) {
////			System.out.println("上下行行为不一致："+segs);
//			return true;
//		}
//
//		// 已过末站，直接去除，segs[12]为当前GPS的下一站
//		int nextStationSeq = Integer.parseInt(segs[12]);
//		// 在初始化文件中，规定当采样点位于末站后，则下一站定为-1。
//		// 判断是否过末站,<1表示过末站
//		if (nextStationSeq < 1) {
////			System.out.println("已过末站："+segs);
//			return true;
//		}
//
//		// 如果JudgeInfo.updownCounter既不等于Constant.DOWN_COUNTER_MIN又不等于Constant.UP_COUNTER_MAX，
//		// 则说明现在正处于上下行纠正的过程中，不进行预报
//		if (ji.getUpdownCounter() != Constant.DOWN_COUNTER_MIN && ji.getUpdownCounter() != Constant.UP_COUNTER_MAX) {
////			System.out.println("上下行行为判断中："+segs);
//			return true;
//		}
//
//		return false;
//	}
//
//}
//
//
