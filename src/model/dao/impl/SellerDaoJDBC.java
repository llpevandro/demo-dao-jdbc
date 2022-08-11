package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db.DB;
import db.DbException;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

public class SellerDaoJDBC implements SellerDao{
	
	//aqui que vai conectar no banco de dados
	
	private Connection conn;
	
	public SellerDaoJDBC(Connection conn) {
		this.conn = conn;
	}

	
	
	@Override
	public void insert(Seller obj) {
		PreparedStatement st = null;
		
		try {
			st = conn.prepareStatement (
					"INSERT INTO seller "
					+ "(Name, Email, BirthDate, BaseSalary, DepartmentId) "
					+ "VALUES "
					+ "(?, ?, ?, ?, ?)", 
					Statement.RETURN_GENERATED_KEYS);
			
			st.setString(1, obj.getName());
			st.setString(2, obj.getEmail());
			st.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
			st.setDouble(4, obj.getBaseSalary());
			st.setInt(5, obj.getDepartment().getId());
			
			int rowsAffected = st.executeUpdate();
			
			if(rowsAffected > 0 ) {
				ResultSet rs = st.getGeneratedKeys();
				if (rs.next()) {
					int id = rs.getInt(1);
					obj.setId(id);
				}
				DB.closeResultSet(rs);
			}
			else {
				throw new DbException("Unexpected error! no rows Affected!");
			}			
		}
		
		catch (SQLException e) {
			throw new DbException(e.getMessage());
		}
		
		finally {
			DB.closeStatement(st);
			
		}
		
		
		
	}

	@Override
	public void update(Seller obj) {
		
		
	}

	@Override
	public void deleteById(Integer id) {
		
		
	}

	@Override
	public Seller findById(Integer id) {
		PreparedStatement st = null;
		ResultSet rs = null;
		
		try {
			st = conn.prepareStatement(
					
					"SELECT seller.*,department.Name as DepName "
					+ "FROM seller INNER JOIN department "
					+ "ON seller.DepartmentId = department.Id "
					+ "WHERE seller.Id = ?");
					
					st.setInt(1, id);
					rs = st.executeQuery();
					//abaixo, testar se veio resultado. Se 'n�o', retorna nulo, se 'sim' passa pela condi��o
					if (rs.next()) {
						Department dep = instantiateDepartment(rs);
						Seller obj = instantiateSeller(rs, dep);						
						return obj;						
					}
					return null;
			}		
		catch(SQLException e){
			throw new DbException(e.getMessage());			
		}		
		finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}

	private Seller instantiateSeller(ResultSet rs, Department dep) throws SQLException {
		Seller obj = new Seller();
		obj.setId(rs.getInt("Id"));
		obj.setName(rs.getString("Name"));
		obj.setEmail(rs.getString("Email"));
		obj.setBaseSalary(rs.getDouble("BaseSalary"));
		obj.setBirthDate(rs.getDate("BirthDate"));
		obj.setDepartment(dep); //pegando o objeto department
		
		return obj;
	}



	private Department instantiateDepartment(ResultSet rs) throws SQLException {
		Department dep = new Department();
		dep.setId(rs.getInt("DepartmentId")); //acessa a coluna DepartmentId
		dep.setName(rs.getString("Depname"));
		return dep;
	}



	@Override
	public List<Seller> findAll() {
		
			PreparedStatement st = null;
			ResultSet rs = null;
			
			try {
				st = conn.prepareStatement(
						
						"SELECT seller.*,department.Name as DepName "
						+ "FROM seller INNER JOIN department "
						+ "ON seller.DepartmentId = department.Id "						
						+ "ORDER BY Name" );
						
						
						rs = st.executeQuery();
						
						List<Seller> list = new ArrayList<>();
						Map<Integer, Department> map = new HashMap<>();
						
						
						while (rs.next()) {
							
							Department dep = map.get(rs.getInt("DepartmentId"));
							
							if (dep == null) {
								dep = instantiateDepartment(rs);
								map.put(rs.getInt("DepartmentId"), dep);
							}
							
							Seller obj = instantiateSeller(rs, dep);
							list.add(obj);
												
						}
						return list;
				}		
			catch(SQLException e){
				throw new DbException(e.getMessage());			
			}		
			finally {
				DB.closeStatement(st);
				DB.closeResultSet(rs);
			}
	}

	//map para controlar a n�o repeti��o de departamento, para ficar somente um dep relacionado aos objetos sallers
	 //testar se o departamento existe

	@Override
	public List<Seller> findByDepartment(Department department) {
		PreparedStatement st = null;
		ResultSet rs = null;
		
		try {
			st = conn.prepareStatement(
					
					"SELECT seller.*,department.Name as DepName "
					+ "FROM seller INNER JOIN department "
					+ "ON seller.DepartmentId = department.Id "
					+ "WHERE DepartmentId = ? "
					+ "ORDER BY Name" );
					
					st.setInt(1, department.getId());
					rs = st.executeQuery();
					
					List<Seller> list = new ArrayList<>();
					Map<Integer, Department> map = new HashMap<>();
					
					
					while (rs.next()) {
						
						Department dep = map.get(rs.getInt("DepartmentId"));
						
						if (dep == null) {
							dep = instantiateDepartment(rs);
							map.put(rs.getInt("DepartmentId"), dep);
						}
						
						Seller obj = instantiateSeller(rs, dep);
						list.add(obj);
											
					}
					return list;
			}		
		catch(SQLException e){
			throw new DbException(e.getMessage());			
		}		
		finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}

}
