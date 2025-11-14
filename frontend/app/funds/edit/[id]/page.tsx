"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";

import { fundApi } from "@/lib/api";
import { Loader2 } from "lucide-react";
import { useParams } from 'next/navigation'
import { useQuery } from '@tanstack/react-query'

export default function EditFundPage() {
  const router = useRouter();
  const params = useParams()
  const fundId = parseInt(params.id as string)

  const { data: fund, isLoading, error } = useQuery({
    queryKey: ['funds'],
    queryFn: () => fundApi.get(fundId)
  })

  const [form, setForm] = useState({
    name: "",
    gp_name: "",
    fund_type: "",
    vintage_year: "",
  });

  const [errors, setErrors] = useState<any>({});
  const [loading, setLoading] = useState(false);

  // -----------------------
  // VALIDATION RULES
  // -----------------------
  const validate = () => {
    const temp: any = {};

    // Value final — jika form belum diubah, pakai value dari fund
    const final = {
      name: form.name.trim() || fund?.name?.trim(),
      gp_name: form.gp_name.trim() || fund?.gp_name?.trim(),
      fund_type: form.fund_type.trim() || fund?.fund_type?.trim(),
      vintage_year: form.vintage_year.trim() || String(fund?.vintage_year || ""),
    };

    // FUND NAME
    if (!final.name) temp.name = "Fund Name is required.";

    // GP NAME
    if (!final.gp_name) temp.gp_name = "GP Name is required.";

    // FUND TYPE
    if (!final.fund_type) temp.fund_type = "Fund Type is required.";

    // VINTAGE YEAR
    if (!final.vintage_year) {
      temp.vintage_year = "Vintage Year is required.";
    } else if (!/^\d{4}$/.test(final.vintage_year)) {
      temp.vintage_year = "Vintage Year must be a 4-digit year.";
    } else if (parseInt(final.vintage_year) < 1900) {
      temp.vintage_year = "Vintage Year must be greater than 1900.";
    }

    setErrors(temp);
    return Object.keys(temp).length === 0;
  };

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setForm({
      ...form,
      [e.target.name]: e.target.value,
    });
  };

  const handleSubmit = async (e: any) => {
    e.preventDefault();
    if (!validate()) return;

    setLoading(true);

    try {
      // FINAL VALUE (fallback ke fund jika form kosong)
      const final = {
        name: form.name.trim() || fund?.name,
        gp_name: form.gp_name.trim() || fund?.gp_name,
        fund_type: form.fund_type.trim() || fund?.fund_type,
        vintage_year: parseInt(
          form.vintage_year.trim() || String(fund?.vintage_year)
        ),
      };

      await fundApi.update(fundId, final);

      window.location.href = "/funds";
    } catch (err) {
      console.error("Failed to update fund:", err);
      alert("Failed to update fund. Please try again.");
    } finally {
      setLoading(false);
    }
  };


  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-8 h-8 animate-spin text-blue-600" />
      </div>
    )
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-4">
        <p className="text-red-800">Error loading funds: {(error as Error).message}</p>
      </div>
    )
  }

  return (
    <div className="max-w-2xl mx-auto mt-10 p-6 bg-white rounded-xl shadow">
      <h1 className="text-2xl font-bold mb-6">Edit Fund</h1>

      <form onSubmit={handleSubmit} className="space-y-5">
        {/* FUND NAME */}
        <div>
          <label className="block font-medium">Fund Name *</label>
          <input
            type="text"
            name="name"
            className="w-full border p-2 rounded"
            value={form.name || fund?.name || ''}
            onChange={handleChange}
          />
          {errors.name && (
            <p className="text-red-500 text-sm">{errors.name}</p>
          )}
        </div>

        {/* GP NAME */}
        <div>
          <label className="block font-medium">GP Name *</label>
          <input
            type="text"
            name="gp_name"
            className="w-full border p-2 rounded"
            value={form.gp_name || fund?.gp_name || ''}
            onChange={handleChange}
          />
          {errors.gp_name && (
            <p className="text-red-500 text-sm">{errors.gp_name}</p>
          )}
        </div>

        {/* FUND TYPE */}
        <div>
          <label className="block font-medium">Fund Type *</label>
          <input
            type="text"
            name="fund_type"
            className="w-full border p-2 rounded"
            value={form.fund_type || fund?.fund_type || ''}
            onChange={handleChange}
          />
          {errors.fund_type && (
            <p className="text-red-500 text-sm">{errors.fund_type}</p>
          )}
        </div>

        {/* VINTAGE YEAR */}
        <div>
          <label className="block font-medium">Vintage Year *</label>
          <input
            type="text"
            name="vintage_year"
            placeholder="2024"
            className="w-full border p-2 rounded"
            value={form.vintage_year || fund?.vintage_year || ''}
            onChange={handleChange}
          />
          {errors.vintage_year && (
            <p className="text-red-500 text-sm">{errors.vintage_year}</p>
          )}
        </div>

        {/* SUBMIT BUTTON */}
        <button
          type="submit"
          disabled={loading}
          className="w-full bg-blue-600 text-white py-2 rounded-lg hover:bg-blue-700 disabled:bg-gray-400 flex items-center justify-center gap-2"
        >
          {loading && <Loader2 className="w-5 h-5 animate-spin" />}
          {loading ? "Saving..." : "Update Fund"}
        </button>
      </form>
    </div>
  );
}
